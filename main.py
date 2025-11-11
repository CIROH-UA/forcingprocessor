from forcingprocessor.processor import prep_ngen_data

def lambda_handler(event,context):
    prep_ngen_data(event)