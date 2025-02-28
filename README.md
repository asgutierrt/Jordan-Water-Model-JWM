# Jordan-Water-Model-JWM-
Jordan Water Model (JWM), incl. the extension for Tanker Water Market Analyses (JWM-T), version 2.0

# Set up runnning environment
1. Create a venv with python 2.7.16 (https://stackoverflow.com/questions/65685217/how-to-create-a-python-2-7-virtual-environment-using-python-3-7)
  1. Install the virtual environments package: C:\Python27\python.exe -m pip install virtualenv
  2. create the virtual environment with C:\Python27\Scripts\virtualenv.exe --python=python2.7 .env
  3. Select the python2.7 interpreter that is on the created .venv
4. **for Windows users:** If you don't have system permissions to activate the .env from powershell, open a Command Prompt tab on VScode and run ```.venv\Scripts\activate```
If succesfully activated, you should see ```(.venv) C:\...``` on the cmd
5. Install requirements with ```pip install -r requirements.txt```

## Scipy dependency issues with Python 2.*
Using an old pip version creates issues installing scipy. See possible fixes [here](https://stackoverflow.com/questions/26575587/cant-install-scipy-through-pip)

# source
Yoon, J., Klassert, C., Selby, P., Lachaut, T., Knox, S., Avisse, N., Harou, J., Tilmant, A., and Gorelick, S. (2023). Jordan Water Model (JWM). Stanford Digital Repository. Available at https://purl.stanford.edu/zw908ds8394.
