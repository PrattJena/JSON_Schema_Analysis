import json
import os
import csv
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import numpy as np
from scipy.stats import gaussian_kde

# If not already imported, import seaborn for the heatmap
import seaborn as sns

schema_keywords = {
    '$schema', '$id', '$ref', '$defs', '$comment', '$anchor',
    '$dynamicRef', '$dynamicAnchor', '$vocabulary', '$recursiveRef',
    '$recursiveAnchor', '$keywords', '$type', '$format', '$title',
    '$description', '$default', '$examples', '$enum', '$const'
}

def load_json_file(file_path):
    """Load a JSON file and return its content."""
    if file_path:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data

'''
---------------------
JSON Schema Functions
---------------------
'''

def extract_top_level_properties(schema):
    properties = set(schema.get('properties', {}).keys())
    return properties - schema_keywords

def extract_objects_with_properties(schema, top_level_properties):

    objects_with_properties = set()
    objects_properties_dict = {}
    for prop in top_level_properties:
        prop_schema = schema['properties'][prop]
        if prop_schema.get('type') == 'object' and 'properties' in prop_schema:

            # Collecting the child property names
            objects_with_properties.add(prop)
            child_props = set(prop_schema['properties'].keys())
            objects_properties_dict[prop] = child_props
    return objects_with_properties, objects_properties_dict

def get_arrays_of_objects_with_properties(schema, top_level_properties, objects_with_properties):

    arrays_properties_dict = {}
    remaining_properties = top_level_properties - objects_with_properties
    for prop in remaining_properties:
        prop_schema = schema['properties'][prop]
        if prop_schema.get('type') == 'array':
            items = prop_schema.get('items', {})
            if items.get('type') == 'object' and 'properties' in items:

                # Collecting the child property names under items
                child_props = set(items['properties'].keys())
                arrays_properties_dict[prop] = child_props
    return arrays_properties_dict

'''
------------------------------
Reference JSON Files Functions
------------------------------
'''

def extract_reference_properties(reference_document):
    return set(reference_document.keys())

def extract_reference_child_properties(reference_document, objects_properties_dict):
    reference_child_properties = dict()
    for property_name in objects_properties_dict:
        prop_value = reference_document.get(property_name)
        if isinstance(prop_value, dict):
            reference_child_properties[property_name] = set(prop_value.keys())
        else:
            reference_child_properties[property_name] = {}

    return reference_child_properties

'''
-----------------------------
Read the innermost JSON files
-----------------------------
'''

def get_innermost_json_files(root_folder, max_folders=None):
    innermost_files = []
    all_subdirs = [name for name in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, name))]
    all_subdirs.sort()

    if max_folders is not None:
        target_folders = all_subdirs[:max_folders]
    else:
        target_folders = all_subdirs

    for target_folder in target_folders:
        folder_path = os.path.join(root_folder, target_folder)
        for dirpath, _, filenames in os.walk(folder_path):
            json_files = [f for f in filenames if f.endswith('.json')]
            for json_file in json_files:
                innermost_files.append(os.path.join(dirpath, json_file))

    return innermost_files

def extract_schema_tag(file_path):
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)
            return data.get("$schema", "No $schema tag found")
        except json.JSONDecodeError:
            return "Invalid JSON format"

def encode_url(url):
    encoded_url = (url.replace("https://", "https__slash__slash_")
                   .replace("http://", "http__slash__slash_")
                   .replace("/", "__slash__")
                   .replace(":", "__colon__")
                   .replace("?", "__question__"))

    # Ensuring file ends with .json and not .json.json
    if encoded_url.endswith(".json"):
        return encoded_url
    else:
        return encoded_url + ".json"

def get_schema_file_path(schema_url, schemas_folder):

    encoded_schema = encode_url(schema_url)
    schema_file_path = os.path.join(schemas_folder, encoded_schema)
    if os.path.isfile(schema_file_path):
        return schema_file_path
    else:
        # Return None instead of raising an error
        return None

"""
----------------------
Get Missing and Extra Properties
----------------------
"""

def find_top_level_properties_difference(innermost_json_files, schemas_folder):
    """
    Find missing and extra top-level properties and write the results to CSV files.
    """
    errors = 0

    # Initialize data structures
    differences = {
        'missing': {},
        'extra': {}
    }
    schema_file_counts = {}

    for file_path in innermost_json_files:
        try:
            schema_tag = extract_schema_tag(file_path)
            if not schema_tag or schema_tag == "No $schema tag found":
                # print(f"File: {file_path}")
                # print("Error: No $schema tag found.\n")
                continue

            schema_file_path = get_schema_file_path(schema_tag, schemas_folder)
            if not schema_file_path:
                continue

            schema = load_json_file(schema_file_path)
            reference_document = load_json_file(file_path)

            if not schema or not reference_document:
                continue

            schema_top_level_props = extract_top_level_properties(schema)
            reference_top_level_props = extract_reference_properties(reference_document)

            # Exclude schema keywords from reference properties
            filtered_reference_props = reference_top_level_props - schema_keywords

            # Find missing and extra properties
            missing_props = schema_top_level_props - filtered_reference_props
            extra_props = filtered_reference_props - schema_top_level_props

            # Update schema file counts
            if schema_tag not in schema_file_counts:
                schema_file_counts[schema_tag] = 0
            schema_file_counts[schema_tag] += 1

            # Update counts for missing properties
            if schema_tag not in differences['missing']:
                differences['missing'][schema_tag] = {}
            for prop in missing_props:
                differences['missing'][schema_tag][prop] = differences['missing'][schema_tag].get(prop, 0) + 1

            # Update counts for extra properties
            if schema_tag not in differences['extra']:
                differences['extra'][schema_tag] = {}
            for prop in extra_props:
                differences['extra'][schema_tag][prop] = differences['extra'][schema_tag].get(prop, 0) + 1

        except Exception as e:
            errors += 1
            continue

    # Write the results to CSV files
    for difference_type in ['missing', 'extra']:
        output_csv = f'{difference_type}_top_level_properties.csv'
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Schema', 'Property', 'Percentage']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for schema_url, props_counts in differences[difference_type].items():
                total_files = schema_file_counts[schema_url]
                for prop, count in props_counts.items():
                    percentage = (count / total_files) * 100 if total_files > 0 else 0.0
                    writer.writerow({'Schema': schema_url, 'Property': prop, 'Percentage': f"{percentage:.2f}"})

        print(f"{errors} errors found")
        print(f"{difference_type.capitalize()} top-level properties percentages have been written to '{output_csv}'")

    return differences, schema_file_counts

# def find_missing_nested_properties(innermost_json_files, schemas_folder):
#     """Find missing nested properties and write the results to a CSV file."""
#
#     missing_nested_props_counts = {}
#     schema_file_counts = {}
#     schemaerror = 0
#     fileerror = 0
#     cantloaddoc = 0
#     noprops = 0
#     errors = 0
#
#     for file_path in innermost_json_files:
#         try:
#             # Extract the $schema tag
#             schema_tag = extract_schema_tag(file_path)
#             if not schema_tag or schema_tag == "No $schema tag found":
#                 schemaerror += 1
#                 continue
#
#             # Get the path of schema mentioned above
#             schema_file_path = get_schema_file_path(schema_tag, schemas_folder)
#             if not schema_file_path:
#                 fileerror += 1
#                 continue
#
#             # Load the schema and the reference document
#             schema = load_json_file(schema_file_path)
#             reference_document = load_json_file(file_path)
#
#             if not schema or not reference_document:
#                 cantloaddoc += 1
#                 continue
#
#             # Get nested properties from schema
#             schema_top_level_props = extract_top_level_properties(schema)
#             objects_with_properties, schema_nested_props_dict = extract_objects_with_properties(schema, schema_top_level_props)
#
#             if not objects_with_properties:
#                 noprops += 1
#                 continue  # No nested properties to compare
#
#             reference_nested_props_dict = extract_reference_child_properties(reference_document, objects_with_properties)
#
#             for obj_prop in objects_with_properties:
#                 schema_nested_props = schema_nested_props_dict.get(obj_prop, set())
#                 reference_nested_props = reference_nested_props_dict.get(obj_prop, set())
#                 missing_nested_props = schema_nested_props - reference_nested_props
#
#                 if missing_nested_props:
#                     if schema_tag not in missing_nested_props_counts:
#                         missing_nested_props_counts[schema_tag] = {}
#                     if schema_tag not in schema_file_counts:
#                         schema_file_counts[schema_tag] = 0
#                     schema_file_counts[schema_tag] += 1
#
#                     for nested_prop in missing_nested_props:
#                         key = (obj_prop, nested_prop)
#                         if key in missing_nested_props_counts[schema_tag]:
#                             missing_nested_props_counts[schema_tag][key] += 1
#                         else:
#                             missing_nested_props_counts[schema_tag][key] = 1
#
#         except Exception as e:
#             errors += 1
#             continue
#
#     # Write the results to a CSV file
#     output_csv = 'missing_nested_properties.csv'
#
#     with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
#         fieldnames = ['Schema', 'Parent_Property', 'Child_Property', 'Percentage']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#
#         writer.writeheader()
#
#         for schema_url, props_counts in missing_nested_props_counts.items():
#             total_files = schema_file_counts.get(schema_url, 0)
#             for (parent_prop, child_prop), count in props_counts.items():
#                 percentage = (count / total_files) * 100 if total_files > 0 else 0.0
#                 writer.writerow({
#                     'Schema': schema_url,
#                     'Parent_Property': parent_prop,
#                     'Child_Property': child_prop,
#                     'Percentage': f"{percentage:.2f}"
#                 })
#
#     print(f"{len(innermost_json_files)} files processed, {noprops} with no child props, "
#           f"{schemaerror} schema tag errors, {fileerror} can't load file errors, {cantloaddoc} can't load doc errors, {errors} errors")
#     print(f"Missing nested properties percentages have been written to '{output_csv}'")

"""
-------------------
Analysis of results
-------------------
"""

def top_level_properties_analysis():
    df = pd.read_csv('missing_top_level_properties.csv', header=0)
    missing_grouped_properties = df[df['Percentage'].astype(float) > 50].groupby('Schema')['Property'].apply(list)
    print("------MISSING PROPERTIES ANALYSIS------")
    for counter, (schema, properties) in enumerate(missing_grouped_properties.items()):
        print(f"{counter} -> {schema}")
        print(", ".join(properties))
        print()

    df = pd.read_csv('extra_top_level_properties.csv', header=0)
    extra_grouped_properties = df[df['Percentage'].astype(float) > 50].groupby('Schema')['Property'].apply(list)
    print("------EXTRA PROPERTIES ANALYSIS------")
    for counter, (schema, properties) in enumerate(extra_grouped_properties.items()):
        print(f"{counter} -> {schema}")
        print(", ".join(properties))
        print()

"""
-----------------------------------------------
Get frequency count of most used property names
-----------------------------------------------
"""

def count_top_level_properties(innermost_json_files):

    property_counts = Counter()
    errors = 0
    for file_path in innermost_json_files:
        try:
            data = load_json_file(file_path)
            if not data:
                continue

            properties = set(data.keys())
            properties = properties - schema_keywords
            property_counts.update(properties)

        except Exception as e:
            errors += 1
            continue

    top_5_properties = property_counts.most_common(5)

    # Separate the property names and counts
    properties = [prop for prop, count in top_5_properties]
    counts = [count for prop, count in top_5_properties]

    # Plotting the horizontal bar graph
    plt.figure(figsize=(5, 2))
    plt.grid()
    plt.barh(properties, counts, color='#F76902')
    plt.xticks(fontsize=6)
    plt.yticks(fontsize=6)
    plt.title('Top 5 Most Used Properties', fontsize=8)

    plt.gca().invert_yaxis()  # Highest count on top
    plt.tight_layout()
    plt.show()

    # Print the top 5 properties with their counts
    print("Top 5 most used properties:")
    print(f"{errors} errors found")
    for prop, count in top_5_properties:
        print(f"{prop}: {count}")

"""
------------------------
Aggregated Result Plots
------------------------
"""

def collect_schema_property_counts(schemas_folder):
    schema_property_counts = []

    for filename in os.listdir(schemas_folder):
        if filename.endswith('.json'):
            filepath = os.path.join(schemas_folder, filename)
            try:
                schema = load_json_file(filepath)
                if not schema:
                    continue
                properties = set(schema.get('properties', {}).keys())
                properties = properties - schema_keywords
                num_properties = len(properties)
                schema_property_counts.append(num_properties)
            except Exception as e:
                continue

    return schema_property_counts

def collect_document_property_counts(innermost_json_files):
    document_property_counts = []
    errors = 0
    for file_path in innermost_json_files:
        try:
            data = load_json_file(file_path)
            if not data:
                continue
            properties = set(data.keys())
            properties = properties - schema_keywords
            num_properties = len(properties)
            document_property_counts.append(num_properties)
        except Exception as e:
            errors += 1
            continue

    return document_property_counts

def plot_property_count_histograms(schema_counts, document_counts):

    # Process schemas
    schema_counts_array = np.array(schema_counts)
    Q1 = np.percentile(schema_counts_array, 25)
    Q3 = np.percentile(schema_counts_array, 75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    filtered_schema_counts = schema_counts_array[(schema_counts_array >= lower_bound) & (schema_counts_array <= upper_bound)]
    num_schemas_excluded = len(schema_counts_array) - len(filtered_schema_counts)
    print(f"Number of schemas excluded as outliers: {num_schemas_excluded}")
    print(f"IQR for schemas: Q1={Q1}, Q3={Q3}, IQR={IQR}, Lower Bound=0, Upper Bound={upper_bound}")

    # Plot histogram for schemas
    plt.figure(figsize=(10, 6))
    bins_schemas = range(int(min(filtered_schema_counts)), int(max(filtered_schema_counts)) + 2)
    plt.hist(filtered_schema_counts, bins=bins_schemas, color='blue', edgecolor='black')
    plt.title('Distribution of Number of Properties in Schemas (Outliers Removed using IQR)')
    plt.xlabel('Number of Properties')
    plt.ylabel('Frequency')
    text_box_content = (
        "Outliers were removed using IQR by excluding values\n"
        "outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR]. This helps\n"
        "focus on typical schema properties."
    )

    plt.text(
        0.95, 0.95, text_box_content, transform=plt.gca().transAxes,
        fontsize=10, verticalalignment='top', horizontalalignment='right',
        bbox=dict(boxstyle="round", facecolor="white", edgecolor="black", alpha=0.5)
    )

    plt.grid(axis='y', alpha=0.75)
    plt.tight_layout()
    plt.show()

    # Process documents
    document_counts_array = np.array(document_counts)
    Q1_doc = np.percentile(document_counts_array, 25)
    Q3_doc = np.percentile(document_counts_array, 75)
    IQR_doc = Q3_doc - Q1_doc
    lower_bound_doc = Q1_doc - 1.5 * IQR_doc
    upper_bound_doc = Q3_doc + 1.5 * IQR_doc
    filtered_document_counts = document_counts_array[(document_counts_array >= lower_bound_doc) & (document_counts_array <= upper_bound_doc)]
    num_documents_excluded = len(document_counts_array) - len(filtered_document_counts)
    print(f"Number of documents excluded as outliers: {num_documents_excluded}")
    print(f"IQR for documents: Q1={Q1_doc}, Q3={Q3_doc}, IQR={IQR_doc}, Lower Bound=0, Upper Bound={upper_bound_doc}")

    # Plot histogram for documents
    plt.figure(figsize=(10, 6))
    bins_documents = range(int(min(filtered_document_counts)), int(max(filtered_document_counts)) + 2)
    plt.hist(filtered_document_counts, bins=bins_documents, color='orange', edgecolor='black')
    plt.title('Distribution of Number of Properties in Documents (Outliers Removed using IQR)')
    plt.xlabel('Number of Properties')
    plt.ylabel('Frequency')
    plt.grid(axis='y', alpha=0.75)
    plt.tight_layout()
    plt.show()

# --- Add the following four functions under this section ---

def plot_property_count_boxplots(schema_counts, document_counts):
    # Box plot for schema counts
    plt.figure(figsize=(6, 6))
    plt.boxplot(schema_counts, showfliers=True)
    plt.title("Box Plot of Property Counts in Schemas")
    plt.ylabel('Number of Properties')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Box plot for document counts
    plt.figure(figsize=(6, 6))
    plt.boxplot(document_counts, showfliers=True)
    plt.title("Box Plot of Property Counts in JSON Documents")
    plt.ylabel('Number of Properties')
    plt.grid(True)
    plt.tight_layout()
    plt.show()



def plot_missing_properties_histogram(innermost_json_files, schemas_folder):
    missing_counts = []
    for file_path in innermost_json_files:
        try:
            schema_tag = extract_schema_tag(file_path)
            if not schema_tag or schema_tag == "No $schema tag found":
                continue

            schema_file_path = get_schema_file_path(schema_tag, schemas_folder)
            if not schema_file_path:
                continue

            schema = load_json_file(schema_file_path)
            reference_document = load_json_file(file_path)

            if not schema or not reference_document:
                continue

            schema_props = extract_top_level_properties(schema)
            doc_props = extract_reference_properties(reference_document) - schema_keywords
            missing_props = schema_props - doc_props
            missing_counts.append(len(missing_props))
        except Exception as e:
            continue

    if not missing_counts:
        print("No missing properties data available.")
        return

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.hist(missing_counts, bins=range(max(missing_counts) + 2), color='#FFA600', edgecolor='black', align='left')
    plt.title('Histogram of Missing Properties per Document')
    plt.xlabel('Number of Missing Properties')
    plt.ylabel('Frequency')
    plt.xticks(range(max(missing_counts) + 1))
    plt.grid(axis='y', alpha=0.75)
    plt.tight_layout()
    plt.show()


def plot_extra_fields_boxplot(innermost_json_files, schemas_folder):
    extra_counts = []

    for file_path in innermost_json_files:

            schema_tag = extract_schema_tag(file_path)
            if not schema_tag or schema_tag == "No $schema tag found":
                continue

            schema_file_path = get_schema_file_path(schema_tag, schemas_folder)
            if not schema_file_path:
                continue

            schema = load_json_file(schema_file_path)
            doc = load_json_file(file_path)
            if not schema or not doc:
                continue

            schema_props = extract_top_level_properties(schema)
            doc_props = set(doc.keys()) - schema_keywords

            # Extra fields are those in doc_props but not in schema_props
            extra_fields = doc_props - schema_props
            extra_count = len(extra_fields)
            extra_counts.append(extra_count)


    if not extra_counts:
        print("No data to plot for extra fields.")
        return

    try:
        # Calculate IQR and outlier bounds
        Q1 = np.percentile(extra_counts, 25)
        Q3 = np.percentile(extra_counts, 75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # Identify outliers
        outliers = [x for x in extra_counts if x < lower_bound or x > upper_bound]
        num_outliers = len(outliers)

        # Print IQR details
        print(f"IQR for documents: Q1={Q1}, Q3={Q3}, IQR={IQR}")
        print(f"Lower Bound={lower_bound}, Upper Bound={upper_bound}")
        print(f"Number of documents excluded as outliers: {num_outliers}")

        # Plot the box plot
        plt.figure(figsize=(6, 6))
        plt.boxplot(extra_counts, showfliers=True)
        plt.title("Box Plot of Extra Fields per Document")
        plt.ylabel('Number of Extra Fields')
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Error while plotting: {e}")


def plot_complexity_vs_missing_with_colormap(differences, schema_file_counts):
    complexities = []
    avg_missing = []
    for schema_url, missing_props in differences['missing'].items():
        total_files = schema_file_counts.get(schema_url, 0)
        if total_files > 0:
            # Number of properties defined in the schema
            schema_file_path = get_schema_file_path(schema_url, schemas_folder)
            if not schema_file_path:
                continue
            schema = load_json_file(schema_file_path)
            if not schema:
                continue
            num_defined_props = len(extract_top_level_properties(schema))

            # Average number of missing properties per document
            total_missing = sum(missing_props.values())
            avg_missing_props = total_missing / total_files

            complexities.append(num_defined_props)
            avg_missing.append(avg_missing_props)

    # Convert to numpy arrays for easier manipulation
    complexities = np.array(complexities)
    avg_missing = np.array(avg_missing)

    # Calculate IQR for average missing values
    q1, q3 = np.percentile(avg_missing, [25, 75])
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # Filter out outliers
    non_outliers = (avg_missing >= lower_bound) & (avg_missing <= upper_bound)
    complexities = complexities[non_outliers]
    avg_missing = avg_missing[non_outliers]

    # Calculate density using Gaussian KDE
    xy = np.vstack([complexities, avg_missing])
    density = gaussian_kde(xy)(xy)

    # Plotting
    plt.figure(figsize=(8, 6))
    scatter = plt.scatter(complexities, avg_missing, c=density, cmap='spring_r', s=50, edgecolor='black')
    plt.colorbar(scatter, label='Density')
    plt.xlabel('Number of Properties Defined in Schema')
    plt.ylabel('Average Number of Missing Properties')
    plt.title('Schema Complexity vs. Missing Properties (With Density)')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# --- End of added functions ---



if __name__ == "__main__":
    root_folder = 'more_fetched_data'
    schemas_folder = 'schemas'
    max_folders = None  # Set to None if all folders

    print("Processing all JSON files")
    innermost_json_files = get_innermost_json_files(root_folder, max_folders=max_folders)

    print("\nProcessing Top Level Properties:-")

    differences, schema_file_counts = find_top_level_properties_difference(innermost_json_files, schemas_folder)
    top_level_properties_analysis()

    # Plot the top 5 missing and extra properties
    # plot_top_properties(differences, schema_file_counts, difference_type='missing', top_n=5)
    # plot_top_properties(differences, schema_file_counts, difference_type='extra', top_n=5)

    count_top_level_properties(innermost_json_files)

    # Collect property counts for schemas and documents
    schema_counts = collect_schema_property_counts(schemas_folder)
    document_counts = collect_document_property_counts(innermost_json_files)

    # Plot histograms of property counts
    plot_property_count_histograms(schema_counts, document_counts)

    # Plot box plots of property counts
    plot_property_count_boxplots(schema_counts, document_counts)

    # Plot scatter plot of schema complexity vs. missing properties
    plot_complexity_vs_missing_with_colormap(differences, schema_file_counts)

    # Plot histogram of missing properties per document
    plot_extra_fields_boxplot(innermost_json_files, schemas_folder)


