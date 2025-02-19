import pandas as pd
import matplotlib.pyplot as plt
import json



def plot_top_schemas(df, top_n):
    schema_counts = df['url'].value_counts().head(top_n)
    print(schema_counts[:10])

    for url in schema_counts:
        print(f"{url[0]} - {url[1]}")

    schema_counts.index = [url.replace('https://', '').replace('http://', '') for url in schema_counts.index]

    schema_counts.index = [url if len(url) < 40 else url[:37] + '...' for url in schema_counts.index]

    plt.figure(figsize=(12, 8))
    schema_counts.plot(kind='bar', color='lightblue')
    plt.title(f'Top {top_n} Most Used Schemas', fontsize=14)
    plt.xlabel('Schema URL')
    plt.ylabel('Number of Occurrences')

    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    plt.show()


def get_most_least_frequent_commits(json_file):
    commits_data = []
    with open(json_file, 'r') as f:
        for line in f:
            commits_data.append(json.loads(line))

    # Convert JSON to pandas DataFrame
    df = pd.DataFrame(commits_data)

    # Explode the 'commits' list into individual rows
    df_exploded = df.explode('commits')

    # Count the number of commits per file path, including repository column
    commit_counts = df_exploded.groupby(['repository', 'path']).size().reset_index(name='commit_count')

    # Sort by commit_count to get top N most and least frequently updated files
    most_frequent = commit_counts.nlargest(10, 'commit_count')
    least_frequent = commit_counts.nsmallest(10, 'commit_count')

    print("Top 10 Most Frequently Updated Files with Repositories:")
    print(most_frequent.to_string(index=False))

    print("\nTop 10 Least Frequently Updated Files with Repositories:")
    print(least_frequent.to_string(index=False))


if __name__ == '__main__':
    # Set options to display the full DataFrame without truncation
    pd.set_option('display.max_rows', None)  # Show all rows
    pd.set_option('display.max_columns', None)  # Show all columns
    pd.set_option('display.expand_frame_repr', False)  # Disable wrapping
    df = pd.read_csv('more_repos_with_json_schema.csv')

    plot_top_schemas(df, top_n=10)
    get_most_least_frequent_commits('commits.json')