FROM gcr.io/deeplearning-platform-release/tf-gpu.2-8
# set the working directory in the container
WORKDIR /code
# copy the dependencies file to the working directory
COPY requirements.txt .
# install dependencies
RUN pip install -r requirements.txt