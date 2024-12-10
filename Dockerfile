# Use a slim Python 3.10 image as the base image
FROM python:3.10-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Copy the content of the app directory into the container
COPY ./app /app

# Install the required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8000
EXPOSE 8000

# Run the application using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
