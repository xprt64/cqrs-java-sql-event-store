docker run --name some-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=pw -d mysql
docker exec -it some-mysql mysql --password pw