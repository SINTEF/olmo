FROM influxdb:1.8.5

# Add miniforge to the image:
WORKDIR /download
RUN wget https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-$(uname)-$(uname -m).sh
RUN bash Mambaforge-$(uname)-$(uname -m).sh -b

# Add the olmo repo:
ADD . /home/olmo
WORKDIR /home/olmo
# Make the output folder (for logs etc)
RUN mkdir -p /home/Output

# Set up/install and activate the olmo_db environment
ENV PATH="/root/mambaforge/bin:$PATH"
RUN conda env create -f ./olmo_db/environment.yml
# Had issues with activating, but it works interactively (just not from this file)
RUN conda init bash
RUN echo "source activate olmo_db" >> ~/.bashrc

# Install the az cli:
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# RUN /root/mambaforge/envs/olmo_db/bin/python backup_influx_to_az.py

# Set up cron jobs:
RUN apt-get update && apt-get -y install cron
COPY olmo_db/cronjobs_influxmachine /etc/cron.d/cronjobs_influxmachine
RUN chmod 0644 /etc/cron.d/cronjobs_influxmachine
RUN touch /var/log/cron_odp.log
RUN touch /var/log/cron_backup.log
# This does not start the deamon, this need to be done after container startup
# using `service cron start`
