import os

# Ensure the .data directory exists
data_dir = 'data/raw/historical'
os.makedirs(data_dir, exist_ok=True)

# List of month names in Catalan
i2m = list(zip(range(1, 13), [
    'Gener', 
    'Febrer', 
    'Marc',
    'Març', 
    'Abril', 
    'Maig', 
    'Juny',
    'Juliol', 
    'Agost', 
    'Setembre', 
    'Octubre', 
    'Novembre', 
    'Desembre'
    ]))

# Loop through each year and month to download and extract data
for year in [2023]:
    for month, month_name in i2m:
        file_name = f'{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'
        file_path = os.path.join(data_dir, file_name)

        # Download the file
        os.system(f"wget -O '{file_path}' 'https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/{file_name}'")

        # Extract the file
        os.system(f"7z x '{file_path}' -o'{data_dir}'")

        # Remove the compressed file
        os.system(f"rm '{file_path}'")
