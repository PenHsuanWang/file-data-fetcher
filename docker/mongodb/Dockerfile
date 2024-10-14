# Dockerfile

# Use the official MongoDB image as the base image
FROM mongo:latest

# Set environment variables for MongoDB root user and password
ENV MONGO_INITDB_ROOT_USERNAME=root
ENV MONGO_INITDB_ROOT_PASSWORD=root

# Set default values for database and collection names
ENV MONGODB_DATABASE=mydatabase
ENV MONGODB_COLLECTION=mycollection

# Copy the shell script to the image
COPY setup_mongodb.sh /setup_mongodb.sh

# Make the shell script executable
RUN chmod +x /setup_mongodb.sh

# Run MongoDB and the setup script in one layer
RUN mongod --fork --logpath /var/log/mongodb.log --bind_ip_all && \
    /setup_mongodb.sh && \
    mongod --shutdown

# Expose the default MongoDB port
EXPOSE 27017

# The base image's entrypoint will start the MongoDB server
