import hashlib

def generate_file_identifier(file_path):
    # Read the file contents
    with open(file_path, 'rb') as file:
        file_contents = file.read()

    # Calculate the SHA-256 hash
    sha256_hash = hashlib.sha256(file_contents).hexdigest()

    return sha256_hash

# List of file paths
file_paths = [
    'processed_demographics.txt',
    'processed_pharmacy.txt',
    'processed_prescription.txt',
    'processed_prior_auth.txt',
    'processed_provider.txt',
    'processed_shipping.txt',
    'processed_third_party.txt'
]

# Generate file identifiers for each file path
file_identifiers = []
for file_path in file_paths:
    file_identifier = generate_file_identifier(file_path)
    file_identifiers.append(file_identifier)

# Print the file identifiers
for file_path, file_identifier in zip(file_paths, file_identifiers):
    print(f"File: {file_path}  File ID: {file_identifier}")