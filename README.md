# CLX - The Experimental Lecture Manager

## Introduction

The Coding Academy Lecture Manager (eXperimental) is a tool to manage
creating course slides. Currently the main functionality it has:

- Creates courses without user interaction (by watching the file system)
- Vscode editor service to edit the course material: [http://localhost:8080]()
- Browser service to view the generated outputs: [http://localhost:8000]()
- JupyterLab service to interact with the course material: 
  [http://localhost:8888]()

Management of videos is planned but currently not implemented.

## Getting Started

- Clone the repository
- Change into the `clx/services` directory
- Run `docker compose build`
- Run `docker compose up`
- Currently, this last command will fail, since the required volume 
  `clx-data` is not properly populated. Until this is fixed, you have to do 
  the following:
  - Run `docker volume create clx-data`
  - Run `docker compose up vscode`
  - Browse to `http://localhost:8080` and create a folder `courses`
  - Change into the `courses` folder and run 
   `git clone git@github.com:hoelzl/PythonCoursesNew.git python` 
    to get the course material
  - Run `docker compose down` or press `Ctrl-C` in the terminal
  - Run `docker compose up` again