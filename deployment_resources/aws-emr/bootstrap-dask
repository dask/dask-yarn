#!/bin/bash
HELP="Usage: bootstrap-dask [OPTIONS]

Example AWS EMR Bootstrap Action to install and configure Dask and Jupyter

By default it does the following things:
- Installs miniconda
- Installs dask, distributed, dask-yarn, pyarrow, and s3fs. This list can be
  extended using the --conda-packages flag below.
- Packages this environment for distribution to the workers.
- Installs and starts a jupyter notebook server running on port 8888. This can
  be disabled with the --no-jupyter flag below.

Options:
    --jupyter / --no-jupyter    Whether to also install and start a Jupyter
                                Notebook Server. Default is True.
    --password, -pw             Set the password for the Jupyter Notebook
                                Server. Default is 'dask-user'.
    --conda-packages            Extra packages to install from conda.
"

set -e

# Parse Inputs. This is specific to this script, and can be ignored
# -----------------------------------------------------------------
JUPYTER_PASSWORD="dask-user"
EXTRA_CONDA_PACKAGES=""
JUPYTER="true"

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "$HELP"
            exit 0
            ;;
        --no-jupyter)
            JUPYTER="false"
            shift
            ;;
        --jupyter)
            JUPYTER="true"
            shift
            ;;
        -pw|--password)
            JUPYTER_PASSWORD="$2"
            shift
            shift
            ;;
        --conda-packages)
            shift
            PACKAGES=()
            while [[ $# -gt 0 ]]; do
                case $1 in
                    -*)
                        break
                        ;;
                    *)
                        PACKAGES+=($1)
                        shift
                        ;;
                esac
            done
            EXTRA_CONDA_PACKAGES="${PACKAGES[@]}"
            ;;
        *)
            echo "error: unrecognized argument: $1"
            exit 2
            ;;
    esac
done


# -----------------------------------------------------------------------------
# 1. Check if running on the master node. If not, there's nothing do.
# -----------------------------------------------------------------------------
grep -q '"isMaster": true' /mnt/var/lib/info/instance.json \
|| { echo "Not running on master node, nothing to do" && exit 0; }


# -----------------------------------------------------------------------------
# 2. Install Miniconda
# -----------------------------------------------------------------------------
echo "Installing Miniconda"
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p $HOME/miniconda
rm /tmp/miniconda.sh
echo -e '\nexport PATH=$HOME/miniconda/bin:$PATH' >> $HOME/.bashrc
source $HOME/.bashrc
conda update conda -y


# -----------------------------------------------------------------------------
# 3. Install packages to use in packaged environment
#
# We install a few packages by default, and allow users to extend this list
# with a CLI flag:
#
# - dask-yarn >= 0.7.0, for deploying Dask on YARN.
# - pyarrow for working with hdfs, parquet, ORC, etc...
# - s3fs for access to s3
# - conda-pack for packaging the environment for distribution
# - ensure tornado 5, since tornado 6 doesn't work with jupyter-server-proxy
# -----------------------------------------------------------------------------
echo "Installing base packages"
conda install \
-c conda-forge \
-y \
-q \
dask-yarn>=0.7.0 \
pyarrow \
s3fs \
conda-pack \
tornado=5 \
$EXTRA_CONDA_PACKAGES


# -----------------------------------------------------------------------------
# 4. Package the environment to be distributed to worker nodes
# -----------------------------------------------------------------------------
echo "Packaging environment"
conda pack -q -o $HOME/environment.tar.gz


# -----------------------------------------------------------------------------
# 5. List all packages in the worker environment
# -----------------------------------------------------------------------------
echo "Packages installed in the worker environment:"
conda list


# -----------------------------------------------------------------------------
# 6. Configure Dask
#
# This isn't necessary, but for this particular bootstrap script it will make a
# few things easier:
#
# - Configure the cluster's dashboard link to show the proxied version through
#   jupyter-server-proxy. This allows access to the dashboard with only an ssh
#   tunnel to the notebook.
#
# - Specify the pre-packaged python environment, so users don't have to
#
# - Set the default deploy-mode to local, so the dashboard proxying works
#
# - Specify the location of the native libhdfs library so pyarrow can find it
#   on the workers and the client (if submitting applications).
# ------------------------------------------------------------------------------
echo "Configuring Dask"
mkdir -p $HOME/.config/dask
cat <<EOT >> $HOME/.config/dask/config.yaml
distributed:
  dashboard:
    link: "/proxy/{port}/status"

yarn:
  environment: /home/hadoop/environment.tar.gz
  deploy-mode: local

  worker:
    env:
      ARROW_LIBHDFS_DIR: /usr/lib/hadoop/lib/native/

  client:
    env:
      ARROW_LIBHDFS_DIR: /usr/lib/hadoop/lib/native/
EOT
# Also set ARROW_LIBHDFS_DIR in ~/.bashrc so it's set for the local user
echo -e '\nexport ARROW_LIBHDFS_DIR=/usr/lib/hadoop/lib/native' >> $HOME/.bashrc


# -----------------------------------------------------------------------------
# 7. If Jupyter isn't requested, we're done
# -----------------------------------------------------------------------------
if [[ "$JUPYTER" == "false" ]]; then
    exit 0
fi


# -----------------------------------------------------------------------------
# 8. Install jupyter notebook server and dependencies
#
# We do this after packaging the worker environments to keep the tar.gz as
# small as possible.
#
# We install the following packages:
#
# - notebook: the Jupyter Notebook Server
# - ipywidgets: used to provide an interactive UI for the YarnCluster objects
# - jupyter-server-proxy: used to proxy the dask dashboard through the notebook server
# -----------------------------------------------------------------------------
if [[ "$JUPYTER" == "true" ]]; then
    echo "Installing Jupyter"
    conda install \
    -c conda-forge \
    -y \
    -q \
    notebook \
    ipywidgets \
    jupyter-server-proxy
fi


# -----------------------------------------------------------------------------
# 9. List all packages in the client environment
# -----------------------------------------------------------------------------
echo "Packages installed in the client environment:"
conda list


# -----------------------------------------------------------------------------
# 10. Configure Jupyter Notebook
# -----------------------------------------------------------------------------
echo "Configuring Jupyter"
mkdir -p $HOME/.jupyter
HASHED_PASSWORD=`python -c "from notebook.auth import passwd; print(passwd('$JUPYTER_PASSWORD'))"`
cat <<EOF >> $HOME/.jupyter/jupyter_notebook_config.py
c.NotebookApp.password = u'$HASHED_PASSWORD'
c.NotebookApp.open_browser = False
c.NotebookApp.ip = '0.0.0.0'
EOF


# -----------------------------------------------------------------------------
# 11. Define an upstart service for the Jupyter Notebook Server
#
# This sets the notebook server up to properly run as a background service.
# -----------------------------------------------------------------------------
echo "Configuring Jupyter Notebook Upstart Service"
cat <<EOF > /tmp/jupyter-notebook.conf
description "Jupyter Notebook Server"
start on runlevel [2345]
stop on runlevel [016]
respawn
respawn limit unlimited
exec su - hadoop -c "jupyter notebook" >> /var/log/jupyter-notebook.log 2>&1
EOF
sudo mv /tmp/jupyter-notebook.conf /etc/init/


# -----------------------------------------------------------------------------
# 12. Start the Jupyter Notebook Server
# -----------------------------------------------------------------------------
echo "Starting Jupyter Notebook Server"
sudo initctl reload-configuration
sudo initctl start jupyter-notebook
