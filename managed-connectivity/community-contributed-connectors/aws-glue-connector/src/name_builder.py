from src.constants import EntryType, SOURCE_TYPE

def create_name(config, entry_type, db_name, asset_name=None):
    """Creates the 'name' for a Dataplex entry within an Entry Group."""
    project = config['project_id']
    location = config['location_id']
    entry_group = config['entry_group_id']
    
    # Sanitize all components
    db_name_sanitized = db_name.replace('-', '_')
    
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        asset_name_sanitized = asset_name.replace('.', '_').replace('-', '_')
        # The entry name for a table should be unique. Combining db and table is robust.
        return f"projects/{project}/locations/{location}/entryGroups/{entry_group}/entries/{db_name_sanitized}_{asset_name_sanitized}"
    
    # Return None for database to signify it should not have a standalone entry
    return None

def create_fqn(config, entry_type, db_name, asset_name=None):
    """Creates the 'fully_qualified_name' for a Table or View."""
    system = SOURCE_TYPE
    
    aws_account_id = config.get('aws_account_id')
    aws_region = config.get('aws_region')

    if not aws_account_id or not aws_region:
        raise ValueError("AWS Account ID and Region are missing from the configuration.")

    # --- THIS IS THE CRITICAL FIX ---
    # Sanitize both region and database names by replacing hyphens.
    # region_sanitized = aws_region.replace('-', '_')
    # db_name_sanitized = db_name.replace('-', '_')

    # FQN is only defined for Tables and Views
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        asset_name_sanitized = asset_name.replace('-', '_')
        path = (f"table:{aws_region}.{aws_account_id}."
                f"{db_name}.{asset_name_sanitized}")
        return f"{system}:{path}"

    # Return None for other types as they don't have a supported FQN
    return None

def create_parent_name(config, entry_type, db_name):
    """Parent Entry is not used in this model as there is no DB entry."""
    return None