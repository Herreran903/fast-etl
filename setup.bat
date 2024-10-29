
REM Activar el entorno virtual
call venv\Scripts\activate

REM Instalar las dependencias
pip install -r requirements.txt

REM Ejecutar el script de Python
python main.py

REM Desactivar el entorno virtual (opcional)
deactivate