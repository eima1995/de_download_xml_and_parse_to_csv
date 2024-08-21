### Project Overview
This project retrieves company data using the Handelsregister API in XML format, processes the XML data, and saves the company details into a CSV file. You can see an example of the final output in the file `240814_Handelsregister-API_Save-Data.xlsx`.

### How to Use
To get started, list the exact names of the companies you want to download in the `company_names.csv` file. The project will use this file to fetch and save the details for each listed company.

### Run
1. Navigate to the project directory:
   ```
   cd download_xml_and_parse_to_csv
   ```
2. ```
   pip install -r requirements.txt
   ```
3. ```
   python handels_register.py
   ```
### Result
Once the project is executed, a file named `handelsregister_result.xlsx` will be generated in the project's root folder. Inside the file, you will find two sheets: "Current output" and "Goal output".

#### Note
The "Current output" sheet adds a new entry for each company listed in company_names.csv every time you run the project. In contrast, the "Goal output" sheet checks if a company is already listed; if it is, the existing row is updated. If the company is not already in the list, a new row is added.

