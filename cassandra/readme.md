`docker run -d --name cassandra-docker -p 9042:9042 -v "${HOME}/docker/volumes/cassandra:/var/lib/cassandra" cassandra`

Since container was created, we can start it right away

`docker start cassandra-docker`