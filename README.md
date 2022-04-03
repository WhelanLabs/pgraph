# Prerequisites for Running the Code
Current test code requires that ArangoDB is started in Docker. 
## steps
1. Start Docker
2. run: docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=openSesame arangodb/arangodb:latest
<!-- run: docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=openSesame arangodb/arangodb:3.8.2 --> 

# Build Commands
   
## To build normally:
   * mvn clean install
   
## To build licensing information:
   * mvn site
   
## To list new versions of third-party components:
   * mvn versions:display-dependency-updates

# Backlog
The Kanban Board for this project is located <a href="notes/kanban.txt">here.</a>