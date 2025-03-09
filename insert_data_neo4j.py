import json
import ast
from neo4j import GraphDatabase

# Connection details for your Neo4j instance.
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "neo4jadmin"

# Establish the Neo4j driver connection.
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def load_json(file_path):
    """Load JSON data from a file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def create_game(tx, game_id, title):
    # Create or merge a Game node using the game_id as the primary key.
    query = """
    MERGE (g:Game {game_id: $game_id})
    SET g.title = $title
    """
    tx.run(query, game_id=game_id, title=title)

def create_publisher(tx, game_id, publisher):
    # Skip over any publisher entries that are simply "None".
    if publisher.strip() == "None":
        return
    query = """
    MATCH (g:Game {game_id: $game_id})
    MERGE (p:Publisher {name: $publisher})
    MERGE (g)-[:PUBLISHED_BY]->(p)
    """
    tx.run(query, game_id=game_id, publisher=publisher)

def create_release(tx, game_id, release_year):
    # Create or merge a Release node and attach it to the Game node.
    query = """
    MATCH (g:Game {game_id: $game_id})
    MERGE (r:Release {year: $release_year})
    MERGE (g)-[:RELEASED_IN]->(r)
    """
    tx.run(query, game_id=game_id, release_year=release_year)

def create_classification_type(tx, game_id, ctype, type_id):
    # Create or merge a Classification node and attach it to the Game node.
    query = """
    MATCH (g:Game {game_id: $game_id})
    MERGE (c:ClassificationType {classification_type_name: $ctype, classification_type_id: $type_id, game_id: $game_id})
    MERGE (g)-[:HAS_CLASSIFICATION_TYPE]->(c)
    """
    tx.run(query, game_id=game_id, ctype=ctype, type_id=type_id)

def create_classification_name(tx, game_id, classification_type_id, classification_name, classification_name_id):
    # Ensure data consistency by stripping whitespace if necessary.
    classification_name = classification_name.strip()
    query = """
    MATCH (g:Game {game_id: $game_id})-[:HAS_CLASSIFICATION_TYPE]->(ct:ClassificationType {classification_type_id: $classification_type_id})
    MERGE (cn:ClassificationName {classification_name: $classification_name, classification_name_id: $classification_name_id})
    MERGE (ct)-[:HAS_CLASSIFICATION_NAME]->(cn)
    """
    tx.run(query, game_id=game_id, classification_type_id=classification_type_id, classification_name=classification_name, classification_name_id=classification_name_id)


def main():
    # Load JSON data from the files.
    game_titles = load_json("data/game_titles_data.json")
    game_publishers = load_json("data/games_publishers_data.json")
    game_releases = load_json("data/games_releases_data.json")
    game_classifications_type = load_json("data/games_classifications_type_data.json")
    game_classifications_value = load_json("data/games_classifications_value_data.json")

    with driver.session() as session:
        # Create Game nodes from titles.
        for game in game_titles:
            session.execute_write(create_game, game["game_id"], game["title"])

        # Create Classification relationships ensuring no duplication.
        for record in game_classifications_type:
            # Strip whitespace to keep data consistent.
            ctype = record["classification_type_name"].strip()
            type_id = record["classification_type_id"]
            session.execute_write(create_classification_type, record["game_id"], ctype, type_id)
        
        for record in game_classifications_value:
            game_id = record["game_id"]
            classification_type_id = record["classification_type_id"]
            classification_name = record["classification_name"].strip()
            classification_name_id = record["game_classification_id"]
            session.execute_write(create_classification_name, game_id, classification_type_id, classification_name, classification_name_id)

        # Create Publisher relationships.
        for record in game_publishers:
            game_id = record["game_id"]
            # Convert the string representation of the list to an actual list.
            try:
                publishers_list = ast.literal_eval(record["publisher"])
            except Exception as e:
                print(f"Error parsing publisher for game_id {game_id}: {e}")
                continue
            for pub in publishers_list:
                session.execute_write(create_publisher, game_id, pub)

        # Create Release relationships.
        for record in game_releases:
            game_id = record["game_id"]
            try:
                release_years = ast.literal_eval(record["release_year"])
            except Exception as e:
                print(f"Error parsing release_year for game_id {game_id}: {e}")
                continue
            for year in release_years:
                session.execute_write(create_release, game_id, year)

    driver.close()

if __name__ == "__main__":
    main()
