import os
import numpy as np
import osmnx as ox
from tqdm import tqdm
from pathlib import Path


def fetch_graph(point, distance=1000):
    north, south, east, west = ox.utils_geo.bbox_from_point(point, dist=distance)
    graph = ox.graph.graph_from_bbox(north, south, east, west, network_type="all_private", 
                                     simplify=False, retain_all=False, truncate_by_edge=True)
    graph = ox.utils_graph.remove_isolated_nodes(graph)
    
    return graph

def transform_graph_to_dataframes(graph):
    gdf_nodes, gdf_relationships = ox.graph_to_gdfs(graph)
    gdf_nodes.reset_index(inplace=True)
    gdf_relationships.reset_index(inplace=True)
    
    relationships = gdf_relationships[["u", "v", "osmid", "length", "geometry"]].copy()
    relationships = relationships.rename(columns={"u": "source", "v": "target"})
    nodes = gdf_nodes[["osmid", "geometry"]].copy()
    edge_table_sql = relationships.copy()
    
    nodes["x"] = nodes.apply(lambda x: x.geometry.coords.xy[0][0], axis=1)
    nodes["y"] = nodes.apply(lambda x: x.geometry.coords.xy[1][0], axis=1)

    edge_table_sql["x1"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[0][0], axis=1)
    edge_table_sql["y1"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[1][0], axis=1)
    edge_table_sql["x2"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[0][1], axis=1)
    edge_table_sql["y2"] = edge_table_sql.apply(lambda x: x.geometry.coords.xy[1][1], axis=1)
    
    relationships = relationships.drop(columns=["geometry"])
    nodes = nodes.drop(columns=["geometry"])
    edge_table_sql = edge_table_sql.drop(columns=["geometry"])
    
    return nodes, relationships, edge_table_sql


def save_to_csv(nodes_df, relationships_df, egde_table, path):
    header = False
    
    if not os.path.exists(os.path.join(path, "nodes_neo4j.csv")):
        header=True
    
    nodes_df.to_csv(os.path.join(path, "nodes_neo4j.csv"), mode="a", index=False, header=header)
    relationships_df.to_csv(os.path.join(path, "relationships_neo4j.csv"), mode="a", index=False, header=header)
    egde_table.to_csv(os.path.join(path, "egde_table_sql.csv"), mode="a", index=False, header=header)
    

def main():
    foldername = "krakow_big"
    csv_paths = f"./data/{foldername}"
    Path(csv_paths).mkdir(exist_ok=True, parents=True)
    os.system(f"rm {csv_paths}/*")
    
    centre_point = (50.064651, 19.944981) # Kraków centre
    whole_bb_length = 500000 # [m]
    north, south, east, west = ox.utils_geo.bbox_from_point(centre_point, dist=whole_bb_length // 2)
    stride = 0.2 # ~20 km
    bb_box_distance = int(stride * 100000 / 1.9) # TODO: check if bigger margin is needed
    
    # remember that west can be greater than east etc.
    lat_arr = np.arange(south, north, stride)
    lon_arr = np.arange(west, east, stride)
    
    coords_mesh_x, coords_mesh_y = np.meshgrid(lat_arr, lon_arr)
    
    for x, y in tqdm(zip(coords_mesh_x.reshape(-1), coords_mesh_y.reshape(-1)), 
                     total=coords_mesh_x.reshape(-1).shape[0], desc="Saving chunk"):
        point = (x, y)
        graph = fetch_graph(point, distance=bb_box_distance)
        nodes, relationships, edge_table_sql = transform_graph_to_dataframes(graph)
        save_to_csv(nodes, relationships, edge_table_sql, csv_paths)
    
    # Drop duplicates
    os.system(f"cd {csv_paths}; awk -F, '!x[$1]++' nodes_neo4j.csv > nodes_neo4j_uniq.csv")
    os.system(f"cd {csv_paths}; awk -F, '!x[$1,$2,$3]++' relationships_neo4j.csv > relationships_neo4j_uniq.csv")
    os.system(f"cd {csv_paths}; awk -F, '!x[$1,$2,$3]++' egde_table_sql.csv > egde_table_sql_uniq.csv")
    os.system(f"cd {csv_paths}; rm nodes_neo4j.csv")
    os.system(f"cd {csv_paths}; rm relationships_neo4j.csv")
    os.system(f"cd {csv_paths}; rm egde_table_sql.csv")
    os.system(f"cd {csv_paths}; mv nodes_neo4j_uniq.csv nodes_neo4j.csv")
    os.system(f"cd {csv_paths}; mv relationships_neo4j_uniq.csv relationships_neo4j.csv")
    os.system(f"cd {csv_paths}; mv egde_table_sql_uniq.csv egde_table_sql.csv")
    
    
if __name__ == "__main__":
    main()
