# Dockerfile
# Use an official Node.js runtime as a base image
FROM node:18

# Set the working directory in the container to /app
WORKDIR /app

# Copy package.json and package-lock.json
COPY ./src/data/scraper/package*.json ./

# Install any needed packages
RUN npm install

# Copy the rest of the application
COPY ./src/data/scraper .

# Make port 8080 available to the world outside this container (optional)
EXPOSE 8080

# Define environment variable (optional)
ENV NAME=app

# Run index.js when the container launches
CMD ["node", "index.js"]