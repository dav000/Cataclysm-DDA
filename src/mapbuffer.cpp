#include "mapbuffer.h"

#include <chrono>
#include <exception>
#include <filesystem>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cata_utility.h"
#include "debug.h"
#include "filesystem.h"
#include "input.h"
#include "json.h"
#include "map.h"
#include "output.h"
#include "overmapbuffer.h"
#include "path_info.h"
#include "popup.h"
#include "string_formatter.h"
#include "submap.h"
#include "translations.h"
#include "ui_manager.h"
#include <future>

#define dbg(x) DebugLog((x),D_MAP) << __FILE__ << ":" << __LINE__ << ": "

class game;
// NOLINTNEXTLINE(cata-static-declarations)
extern std::unique_ptr<game> g;
// NOLINTNEXTLINE(cata-static-declarations)
extern const int savegame_version;

static cata_path find_quad_path( const cata_path &dirname, const tripoint_abs_omt &om_addr )
{
    return dirname / string_format( "%d.%d.%d.map", om_addr.x(), om_addr.y(), om_addr.z() );
}

static cata_path find_dirname( const tripoint_abs_omt &om_addr )
{
    const tripoint_abs_seg segment_addr = project_to<coords::seg>( om_addr );
    return PATH_INFO::world_base_save_path_path() / "maps" / string_format( "%d.%d.%d",
            segment_addr.x(),
            segment_addr.y(), segment_addr.z() );
}

mapbuffer MAPBUFFER;

mapbuffer::mapbuffer() = default;
mapbuffer::~mapbuffer() = default;

void mapbuffer::clear()
{
    submaps.clear();
}

void mapbuffer::clear_outside_reality_bubble()
{
    map &here = get_map();
    auto it = submaps.begin();
    while( it != submaps.end() ) {
        if( here.inbounds( it->first ) ) {
            ++it;
        } else {
            it = submaps.erase( it );
        }
    }
}

bool mapbuffer::add_submap( const tripoint_abs_sm &p, std::unique_ptr<submap> &sm )
{
    if( submaps.count( p ) ) {
        return false;
    }

    submaps[p] = std::move( sm );

    return true;
}

bool mapbuffer::add_submap( const tripoint_abs_sm &p, submap *sm )
{
    // FIXME: get rid of this overload and make submap ownership semantics sane.
    std::unique_ptr<submap> temp( sm );
    bool result = add_submap( p, temp );
    if( !result ) {
        // NOLINTNEXTLINE( bugprone-unused-return-value )
        temp.release();
    }
    return result;
}

void mapbuffer::remove_submap( const tripoint_abs_sm &addr )
{
    auto m_target = submaps.find( addr );
    if( m_target == submaps.end() ) {
        debugmsg( "Tried to remove non-existing submap %s", addr.to_string() );
        return;
    }
    submaps.erase( m_target );
}

submap *mapbuffer::lookup_submap( const tripoint_abs_sm &p )
{
    dbg( D_INFO ) << "mapbuffer::lookup_submap( x[" << p.x() << "], y[" << p.y() << "], z["
                  << p.z() << "])";

    const auto iter = submaps.find( p );
    if( iter == submaps.end() ) {
        try {
            return unserialize_submaps( p );
        } catch( const std::exception &err ) {
            debugmsg( "Failed to load submap %s: %s", p.to_string(), err.what() );
        }
        return nullptr;
    }

    return iter->second.get();
}

bool mapbuffer::submap_exists( const tripoint_abs_sm &p )
{
    const auto iter = submaps.find( p );
    if( iter == submaps.end() ) {
        try {
            return unserialize_submaps( p );
        } catch( const std::exception &err ) {
            debugmsg( "Failed to load submap %s: %s", p.to_string(), err.what() );
        }
        return false;
    }

    return true;
}

void mapbuffer::save(bool delete_after_save)
{
    assure_dir_exist(PATH_INFO::world_base_save_path() + "/maps");

    int num_total_submaps = submaps.size();

    map& here = get_map();

    static_popup popup;

    std::set<tripoint_abs_omt> saved_submaps;
    std::list<tripoint_abs_sm> submaps_to_delete;
    static constexpr std::chrono::milliseconds update_interval(500);

    std::vector<std::future<void>> futures;
    std::mutex submaps_mutex;

    for (auto& elem : submaps) {
        const tripoint_abs_omt om_addr = project_to<coords::omt>(elem.first);
        if (saved_submaps.count(om_addr) != 0) {
            continue;
        }
        saved_submaps.insert(om_addr);

        const cata_path dirname = find_dirname(om_addr);
        const cata_path quad_path = find_quad_path(dirname, om_addr);

        bool inside_reality_bubble = here.inbounds(om_addr);

        futures.push_back(std::async(std::launch::async, [this, dirname, quad_path, om_addr, &submaps_to_delete, delete_after_save, inside_reality_bubble, &submaps_mutex]() {
            std::list<tripoint_abs_sm> local_submaps_to_delete;
            save_quad(dirname, quad_path, om_addr, local_submaps_to_delete, delete_after_save || !inside_reality_bubble, submaps_mutex);

            {
                std::lock_guard<std::mutex> lock(submaps_mutex);
                submaps_to_delete.splice(submaps_to_delete.end(), local_submaps_to_delete);
            }
            }));
    }
    
    int num_saved_submaps = 0;
    auto last_update = std::chrono::steady_clock::now();

    for (auto& fut : futures) {
        if (std::chrono::steady_clock::now() - last_update > update_interval) {
            popup.message(_("Please wait as the map saves [%d/%d]"),
                num_saved_submaps, num_total_submaps);
            ui_manager::redraw();
            refresh_display();
            inp_mngr.pump_events();
            last_update = std::chrono::steady_clock::now();
        }
        num_saved_submaps++;
        fut.get();
    }

    for (auto& elem : submaps_to_delete) {
        remove_submap(elem);
    }
}


void mapbuffer::save_quad(
    const cata_path dirname, const cata_path filename, const tripoint_abs_omt om_addr,
    std::list<tripoint_abs_sm>& submaps_to_delete, bool delete_after_save, std::mutex& submaps_mutex)
{
    static const std::vector<point> offsets = { point_zero, point_south, point_east, point_south_east };
    
    std::vector<tripoint_abs_sm> submap_addrs;
    for (const auto& offset : offsets) {
        submap_addrs.emplace_back(project_to<coords::sm>(om_addr) + offset);
    }

    bool all_uniform = true;
    bool reverted_to_uniform = false;
    bool const file_exists = fs::exists(filename.get_unrelative_path());
    for (const auto& submap_addr : submap_addrs) {
        submap* sm = submaps[submap_addr].get();
        if (sm != nullptr) {
            if (!sm->is_uniform()) {
                all_uniform = false;
            }
            else if (sm->reverted) {
                reverted_to_uniform = file_exists;
            }
        }
    }

    if (all_uniform) {
        if (delete_after_save) {
            std::lock_guard<std::mutex> lock(submaps_mutex);
            for (const auto& submap_addr : submap_addrs) {
                if (submaps.count(submap_addr) > 0 && submaps[submap_addr] != nullptr) {
                    submaps_to_delete.push_back(submap_addr);
                }
            }
        }

        if (!reverted_to_uniform) {
            return;
        }
    }

    assure_dir_exist(dirname);
    write_to_file(filename, [&](std::ostream& fout) {
        JsonOut jsout(fout);
        jsout.start_array();
        for (auto& submap_addr : submap_addrs) {
            if (submaps.count(submap_addr) == 0) {
                continue;
            }

            submap* sm = submaps[submap_addr].get();

            if (sm == nullptr) {
                continue;
            }

            jsout.start_object();

            jsout.member("version", savegame_version);
            jsout.member("coordinates");

            jsout.start_array();
            jsout.write(submap_addr.x());
            jsout.write(submap_addr.y());
            jsout.write(submap_addr.z());
            jsout.end_array();

            sm->store(jsout);

            jsout.end_object();

            if (delete_after_save) {
                std::lock_guard<std::mutex> lock(submaps_mutex);
                submaps_to_delete.push_back(submap_addr);
            }
        }

        jsout.end_array();
        });

    if (all_uniform && reverted_to_uniform) {
        fs::remove(filename.get_unrelative_path());
    }
}

// We're reading in way too many entities here to mess around with creating sub-objects and
// seeking around in them, so we're using the json streaming API.
submap *mapbuffer::unserialize_submaps( const tripoint_abs_sm &p )
{
    // Map the tripoint to the submap quad that stores it.
    const tripoint_abs_omt om_addr = project_to<coords::omt>( p );
    const cata_path dirname = find_dirname( om_addr );
    cata_path quad_path = find_quad_path( dirname, om_addr );

    if( !file_exist( quad_path ) ) {
        // Fix for old saves where the path was generated using std::stringstream, which
        // did format the number using the current locale. That formatting may insert
        // thousands separators, so the resulting path is "map/1,234.7.8.map" instead
        // of "map/1234.7.8.map".
        std::ostringstream buffer;
        buffer << om_addr.x() << "." << om_addr.y() << "." << om_addr.z()
               << ".map";
        cata_path legacy_quad_path = dirname / buffer.str();
        if( file_exist( legacy_quad_path ) ) {
            quad_path = std::move( legacy_quad_path );
        }
    }

    if( !read_from_file_optional_json( quad_path, [this]( const JsonValue & jsin ) {
    deserialize( jsin );
    } ) ) {
        // If it doesn't exist, trigger generating it.
        return nullptr;
    }
    // fill in uniform submaps that were not serialized
    oter_id const oid = overmap_buffer.ter( om_addr );
    generate_uniform_omt( project_to<coords::sm>( om_addr ), oid );
    if( submaps.count( p ) == 0 ) {
        debugmsg( "file %s did not contain the expected submap %s for non-uniform terrain %s",
                  quad_path.generic_u8string(), p.to_string(), oid.id().str() );
        return nullptr;
    }
    return submaps[ p ].get();
}

void mapbuffer::deserialize( const JsonArray &ja )
{
    for( JsonObject submap_json : ja ) {
        std::unique_ptr<submap> sm = std::make_unique<submap>();
        tripoint_abs_sm submap_coordinates;
        int version = 0;
        // We have to read version first because the iteration order of json members is undefined.
        if( submap_json.has_int( "version" ) ) {
            version = submap_json.get_int( "version" );
        }
        for( JsonMember submap_member : submap_json ) {
            std::string submap_member_name = submap_member.name();
            if( submap_member_name == "coordinates" ) {
                JsonArray coords_array = submap_member;
                tripoint_abs_sm loc{ coords_array.next_int(), coords_array.next_int(), coords_array.next_int() };
                submap_coordinates = loc;
            } else {
                sm->load( submap_member, submap_member_name, version );
            }
        }

        if( !add_submap( submap_coordinates, sm ) ) {
            debugmsg( "submap %s was already loaded", submap_coordinates.to_string() );
        }
    }
}
