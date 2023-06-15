from data_loader_module import load_data

#file_path = r'C:\Users\DELL\Desktop\postgres\processed_prior_auth.txt'
file_paths = [
        r'C:\Users\DELL\Desktop\postgres\processed_demographics.txt',
        r'C:\Users\DELL\Desktop\postgres\processed_pharmacy.txt',
        r'C:\Users\DELL\Desktop\postgres\processed_prescription.txt',
        r'C:\Users\DELL\Desktop\postgres\processed_prior_auth.txt',
        r'C:\Users\DELL\Desktop\postgres\processed_provider.txt',
        r'C:\Users\DELL\Desktop\postgres\processed_shipping.txt',
        r'C:\Users\DELL\Desktop\postgres\processed_third_party.txt'

    ]

load_data(file_paths)
print("Data loaded successfully !")

