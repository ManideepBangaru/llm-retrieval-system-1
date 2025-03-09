import os
from langchain_community.graphs import Neo4jGraph
from langchain.chains import GraphCypherQAChain
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Neo4j Connection
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Initialize Neo4j graph
graph = Neo4jGraph(
    url=NEO4J_URI,
    username=NEO4J_USERNAME,
    password=NEO4J_PASSWORD
)

# Initialize OpenAI Model
llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)

# LangChain QA Chain with Neo4j
qa_chain = GraphCypherQAChain.from_llm(
    llm=llm,
    graph=graph,
    return_intermediate_steps=True,
    allow_dangerous_requests=True
)

def query_neo4j(question):
    """Query Neo4j using natural language."""
    response = qa_chain.invoke({"query": question})
    return response

# Example Queries
if __name__ == "__main__":
    question = "What are the classifications of the game with id 5?, show me in json format maintaining the original structure"
    result = query_neo4j(question)
    # export the result to a json file
    with open("data/result.json", "w") as f:
        json.dump(result, f)
