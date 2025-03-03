# Jordan-Water-Model-JWM-
Jordan Water Model (JWM), incl. the extension for Tanker Water Market Analyses (JWM-T), version 2.0

# Set up runnning environment
1. Create a venv [as described here](https://medium.com/@dipan.saha/managing-git-repositories-with-vscode-setting-up-a-virtual-environment-62980b9e8106)
4. **for Windows users:** If you don't have system permissions to activate the .env from powershell, open a Command Prompt tab on VScode and run ```.venv\Scripts\activate```
If succesfully activated, you should see ```(.venv) C:\...``` on the cmd
5. Update pip with ```pip install --upgrade pip``` and Install requirements with ```pip install -r requirements.txt```
6. Use sys.path to Check Where Python is Looking for Module, run ```python -c "import sys; print(sys.path)"```
7. Check if JWM file path is in the list. If it is not in the list, Python doesn't recognize your project folder as a module location. You can force Python to use the correct path by setting the PYTHONPATH environment variable before running your script, by running ```set PYTHONPATH=C:\Users\AFN\Documents\GitHub\Jordan-Water-Model-JWM``` on the command promot tab.

# connect optimization software
https://www.youtube.com/watch?v=I3tIPQn6z5U

# source
Yoon, J., Klassert, C., Selby, P., Lachaut, T., Knox, S., Avisse, N., Harou, J., Tilmant, A., and Gorelick, S. (2023). Jordan Water Model (JWM). Stanford Digital Repository. Available at https://purl.stanford.edu/zw908ds8394.
