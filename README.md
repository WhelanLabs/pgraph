# KGraph

# Prerequisites for Running the Code
Current test code requires that ArangoDB is started in Docker. 

	[Start Docker]
	docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=openSesame arangodb/arangodb:latest
<!-- run: docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=openSesame arangodb/arangodb:3.8.2 --> 

# Build Commands
   
## To build normally:
use one of the following:

	mvn clean source:jar javadoc:jar install
	
	mvn clean source:jar javadoc:jar install -Dtest=\!StockDataLoaderTest*
	
	mvn clean source:jar javadoc:jar install -DskipTests
   
## To build licensing information:
	mvn site

## To build Javadoc
	mvn javadoc:javadoc
   
## To list new versions of third-party components:
	mvn versions:display-dependency-updates

# Backlog
The Kanban Board for this project is located <a href="notes/kanban.txt">here.</a>