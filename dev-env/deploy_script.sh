
#this script runs inside the container. 
#It clones the repository and then checkout for the specific commit if added. 
#After, it runs the cont_execute script in the brie repository. 

echo "commit_version $COMMIT_HASH"
echo "db_port $DB_PORT"
echo "http_port $HTTP_PORT"

docker run --name postgres-db -e POSTGRES_PASSWORD=docker -p 5432:5432 -d postgres

git clone git@github.com:eeg-sense/containerise-cloud-environment.git

git clone git@github.com:eeg-sense/kosmik.git
git fetch
[[ ! -z "$COMMIT_HASH" ]] && git checkout $COMMIT_HASH


#create a DB in the container and then copy all the data from prod to the DB
#run the serverless offline 

python3 cont_execute.py