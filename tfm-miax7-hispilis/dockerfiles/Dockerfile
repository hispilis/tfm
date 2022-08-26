FROM tensorflow/tensorflow:2.8.0
# latest-gpu
COPY dockerfiles/requirements.txt /tmp/

ARG USERNAME=miax7-developer
ARG USER_UID=8989
ARG USER_GID=$USER_UID

# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    #
    # [Optional] Add sudo support. Omit if you don't need to install software after connecting.
    && apt-get update -y \
    #	&& apt-get install -y xvfb \
    #	&& apt-get install -y python3-opengl \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

RUN apt-get install git -y sudo

# RUN	apt-get install swig -y sudo

RUN set -ex; \
    pip3 --no-cache-dir install --requirement /tmp/requirements.txt

USER $USERNAME
ENV PYTHONPATH "/workspaces/tfm-miax7/"

RUN git config --global --add safe.directory /workspaces/tfm-miax7
RUN git config --global core.filemode false
RUN git config --global core.autocrlf true
