FROM codercom/code-server:latest

# Set environment variables
ENV PASSWORD=vscode

# Expose port for the VSCode server
EXPOSE 8080

# Install Python and common Python packages
RUN sudo apt-get update && \
    sudo apt-get install -y python3 python3-pip \
    python3-numpy python3-scipy python3-matplotlib python3-pandas python3-pytest \
    python3-ipython

# Run the VSCode server
CMD ["code-server", "--bind-addr", "0.0.0.0:8080", "--auth", "password"]

