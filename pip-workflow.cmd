:: pip-workflow.cmd

:: 1. Maak een virtuele omgeving genaamd "env"
python -m venv env

:: 2. Activeer de virtuele omgeving
env\Scripts\activate

:: 3. Bekijk de algemene help voor pip
pip help

:: 4. Bekijk help voor pip install
pip help install

:: 5. Installeer het requests-pakket
pip install requests

:: 6. Toon de lijst van geïnstalleerde packages
pip list

:: 7. Probeer beschikbare versies van requests te bekijken (geeft foutmelding met versielijst)
pip install requests==

:: 8. Installeer een specifieke oudere versie van requests
pip install requests==2.25.1

:: 9. Controleer of de juiste versie geïnstalleerd is
pip list

:: 10. Upgrade naar de nieuwste versie van requests
pip install requests --upgrade

:: 11. Controleer de nieuwste versie opnieuw
pip list

:: 12. Verlaat de virtuele omgeving
deactivate
