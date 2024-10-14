#!/bin/bash

# setup_mongodb.sh

echo "Initializing MongoDB database and collection during build..."

# Wait for MongoDB to start
sleep 5

# Create the database and collection
mongo --eval "
  db = db.getSiblingDB('$MONGODB_DATABASE');
  db.createCollection('$MONGODB_COLLECTION');
"

echo "Database '$MONGODB_DATABASE' and collection '$MONGODB_COLLECTION' initialized."
