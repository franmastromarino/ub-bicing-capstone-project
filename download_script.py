import os

i2m = list(zip(range(1,13), ['Gener','Febrer','Marc','Abril','Maig','Juny','Juliol','Agost','Setembre','Octubre','Novembre','Desembre']))
for year in [2023, 2022, 2021, 2020]:
    for month, month_name in i2m:        
        os.system(f"wget 'https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'")
        os.system(f"7z x '{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'")
        os.system(f"rm '{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'")