import subprocess
import os
import sys


# 실행할 스크립트 경로
print("Current Working Directory:", os.getcwd())
script_path = 'models/insight_g/sea_freight/service_exp.py'
script_path2 = 'models/insight_g/sea_freight/service_imp.py'
script_path3 = 'models/insight_g/air_freight/service_exp.py'
script_path4 = 'models/insight_g/air_freight/service_imp.py'
script_path5 = 'models/insight_g/sea_freight_index/service.py'

# 스크립트 실행
subprocess.run([sys.executable, script_path])
subprocess.run([sys.executable, script_path2])
subprocess.run([sys.executable, script_path3])
subprocess.run([sys.executable, script_path4])
subprocess.run([sys.executable, script_path5])