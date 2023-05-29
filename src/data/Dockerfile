# Use an official Python runtime as the base image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY ./srequirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the retrieve_data.py script into the container
COPY ./src/data/retrieve_data.py ./

# Set the environment variable for the config file path
ENV CONFIG_PATH=/app/config.py

# Run the retrieve_data.py script
CMD [ "python", "retrieve_data.py" ]
