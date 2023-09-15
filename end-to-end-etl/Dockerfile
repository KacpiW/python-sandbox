# Use an official Python runtime as a parent image
FROM python:3.11-slim-buster

# Set up environment
ENV POETRY_VERSION=1.5.1
ENV POETRY_VIRTUALENVS_IN_PROJECT=false

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir poetry==$POETRY_VERSION

# Configure Poetry to not create a virtual environment
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-interaction --no-ansi

# Make sure the entrypoint script is executable
ENV PATH="app/.venv/bin:$PATH"

# Run the command to start your application when the container launches.
# This will be the name of your python script.
CMD ["python3", "/app/src/end_to_end/main.py"]
