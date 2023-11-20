import os
import numpy as np
import osmnx as ox
import geopy.distance
from tqdm import tqdm
from pathlib import Path


def fetch_graph(bb_box, margin=0.01):
    north, south, east, west = bb_box
    
    # Margin for safety
    if north > south:
        north += margin
        south -= margin
    else:
        south += margin
        north -= margin
        
    if east > west:
        east += margin
        west -= margin
    else:
        west += margin
        east -= margin
    
    graph = ox.graph.graph_from_bbox(north, south, east, west, network_type="all", 
                                     simplify=False, retain_all=True, truncate_by_edge=True)
    
    return graph


def transform_graph_to_dataframes(graph, centre_point, max_distance):
    gdf_nodes, gdf_relationships = ox.graph_to_gdfs(graph)
    gdf_nodes.reset_index(inplace=True)
    gdf_relationships.reset_index(inplace=True)
    
    relationships = gdf_relationships[["u", "v", "osmid", "length", "name", "highway", "geometry"]].copy()
    relationships = relationships.rename(columns={"u": "source", "v": "target"})
    nodes = gdf_nodes[["osmid", "geometry"]].copy()
    edge_table_sql = relationships.copy()
    
    nodes["x"] = nodes.apply(lambda x: x.geometry.coords.xy[0][0], axis=1)
    nodes["y"] = nodes.apply(lambda x: x.geometry.coords.xy[1][0], axis=1)

    edge_table_sql["x1"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[0][0], axis=1)
    edge_table_sql["y1"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[1][0], axis=1)
    edge_table_sql["x2"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[0][1], axis=1)
    edge_table_sql["y2"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[1][1], axis=1)
    
    # x małe, y duże
    nodes["distance"] = nodes.apply(lambda x: int(max(
                                    np.floor(geopy.distance.distance(centre_point, (x["y"], centre_point[1])).km / max_distance * 4.9999),
                                    np.floor(geopy.distance.distance(centre_point, (centre_point[0], x["x"])).km / max_distance * 4.9999))),
                                    axis=1)
    edge_table_sql["source_distance"] = edge_table_sql.apply(lambda x: int(max(
                                    np.floor(geopy.distance.distance(centre_point, (x["y1"], centre_point[1])).km / max_distance * 4.9999),
                                    np.floor(geopy.distance.distance(centre_point, (centre_point[0], x["x1"])).km / max_distance * 4.9999))),
                                    axis=1)
    edge_table_sql["target_distance"] = edge_table_sql.apply(lambda x: int(max(
                                    np.floor(geopy.distance.distance(centre_point, (x["y2"], centre_point[1])).km / max_distance * 4.9999),
                                    np.floor(geopy.distance.distance(centre_point, (centre_point[0], x["x2"])).km / max_distance * 4.9999))),
                                    axis=1)
    
    nodes = nodes.drop(columns=["geometry"])
    relationships = relationships.drop(columns=["geometry"])
    edge_table_sql = edge_table_sql.drop(columns=["geometry"])
    
    return nodes, relationships, edge_table_sql


def save_to_csv(nodes_df, relationships_df, edge_table, path):
    header = False
    
    if not os.path.exists(os.path.join(path, "nodes_neo4j.csv")):
        header=True
    
    nodes_df.to_csv(os.path.join(path, "nodes_neo4j.csv"), mode="a", index=False, header=header)
    relationships_df.to_csv(os.path.join(path, "relationships_neo4j.csv"), mode="a", index=False, header=header)
    edge_table.to_csv(os.path.join(path, "edge_table_sql.csv"), mode="a", index=False, header=header)
    

def main():
    foldername = "krakow_big"
    csv_paths = f"./data/{foldername}"
    Path(csv_paths).mkdir(exist_ok=True, parents=True)
    os.system(f"rm {csv_paths}/*")
    
    centre_point = (50.064651, 19.944981) # Kraków centre
    bb_dist_from_centre = 150000 # [m]
    north, south, east, west = ox.utils_geo.bbox_from_point(centre_point, dist=bb_dist_from_centre)
    chunks = 10 # chunks in each dimension
    margin = 0.001
    
    # remember that west can be greater than east etc.
    max_distance = int(max(geopy.distance.distance(centre_point, (north + margin, centre_point[1])).km,
                           geopy.distance.distance(centre_point, (south - margin, centre_point[1])).km,
                           geopy.distance.distance(centre_point, (centre_point[0], west - margin)).km,
                           geopy.distance.distance(centre_point, (centre_point[0], east + margin)).km))
    
    print(f"Max distance from centre point: {max_distance}")
    
    # remember that west can be greater than east etc.
    lat_arr = np.linspace(south, north, chunks)
    lon_arr = np.linspace(west, east, chunks)
    
    mesh_x, mesh_y = np.meshgrid(lat_arr, lon_arr)
    mesh_x_1 = mesh_x[:-1, :-1].reshape(-1)
    mesh_x_2 = mesh_x[1:, 1:].reshape(-1)
    mesh_y_1 = mesh_y[:-1, :-1].reshape(-1)
    mesh_y_2 = mesh_y[1:, 1:].reshape(-1)
    
    for x1, x2, y1, y2 in tqdm(zip(mesh_x_1, mesh_x_2, mesh_y_1, mesh_y_2), 
                     total=mesh_x_1.shape[0], desc="Saving chunk"):
        bb_box = (x1, x2, y1, y2)
        graph = fetch_graph(bb_box, margin)
        nodes, relationships, edge_table_sql = transform_graph_to_dataframes(graph, centre_point, max_distance)
        save_to_csv(nodes, relationships, edge_table_sql, csv_paths)
    
    # Size before removing duplicates
    print("Before:")
    os.system(f"cd {csv_paths}; ls -lh")
    
    # Drop duplicates
    os.system(f"cd {csv_paths}; awk -F, '!x[$1]++' nodes_neo4j.csv > nodes_neo4j_uniq.csv")
    os.system(f"cd {csv_paths}; awk -F, '!x[$1,$2,$3]++' relationships_neo4j.csv > relationships_neo4j_uniq.csv")
    os.system(f"cd {csv_paths}; awk -F, '!x[$1,$2,$3]++' edge_table_sql.csv > edge_table_sql_uniq.csv")
    os.system(f"cd {csv_paths}; rm nodes_neo4j.csv")
    os.system(f"cd {csv_paths}; rm relationships_neo4j.csv")
    os.system(f"cd {csv_paths}; rm edge_table_sql.csv")
    os.system(f"cd {csv_paths}; mv nodes_neo4j_uniq.csv nodes_neo4j.csv")
    os.system(f"cd {csv_paths}; mv relationships_neo4j_uniq.csv relationships_neo4j.csv")
    os.system(f"cd {csv_paths}; mv edge_table_sql_uniq.csv edge_table_sql.csv")
    
    # Size after removing duplicates
    print("After:")
    os.system(f"cd {csv_paths}; ls -lh")
    
    
if __name__ == "__main__":
    main()