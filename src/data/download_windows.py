import os

i2m = list(zip(range(1,13), ['Gener','Febrer','Marc','Abril','Maig','Juny','Juliol','Agost','Setembre','Octubre','Novembre','Desembre']))
for year in [2023, 2022, 2021, 2020]:
    for month, month_name in i2m:
        # Download the file using PowerShell's Invoke-WebRequest
        os.system(f"powershell -command \"Invoke-WebRequest -Uri 'https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z' -OutFile '{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'\"")
        
        # Extract the file with 7-Zip
        os.system(f"7z x '{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z' -y")
        
        # Delete the archive after extracting
        os.system(f"del '{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'")