import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import json as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import psycopg2
import matplotlib
from matplotlib import colormaps
import matplotlib.pyplot as plt
import webbrowser
import threading
import os

# browser_opened = False  # Flag to track if the browser is opened

# def open_browser():
#     global browser_opened
#     time.sleep(3)  # Wait for Streamlit to start
#     if not browser_opened:
#         webbrowser.open("http://localhost:8501")
#         browser_opened = True  # Mark browser as opened

# # Run browser opener in a separate thread to prevent blocking
# threading.Thread(target=open_browser, daemon=True).start()


# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    # Poll Kafka consumer for messages within a timeout period
    messages = consumer.poll(timeout_ms=2000)
    data = []

    # Extract data from received messages
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

# Function to fetch voting statistics from PostgreSQL database
@st.cache_data(ttl=5)
def fetch_voting_stats():
    # Connect to PostgreSQL database
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute("""
        SELECT count(*) voters_count FROM voters
    """)
    voters_count = cur.fetchone()[0]

    # Fetch total number of candidates
    cur.execute("""
        SELECT count(*) candidates_count FROM candidates
    """)
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count

# Function to plot a colored bar chart for vote counts per candidate
# Improved colored bar chart function with optional customization
def plot_colored_bar_chart(results, bar_orientation='vertical', color_map='viridis', add_values=True):
    if results.empty:
        print("No data available.")
        return None

    data_type = results['candi_name']
    total_votes = results['Total_Votes']
    num_bars = len(data_type)  # Number of bars to adjust layout dynamically
    colors = matplotlib.colormaps[color_map](np.linspace(0, 1, num_bars))

    # Dynamically adjust figure size based on the number of bars
    fig_width = max(8, num_bars / 3)  # Base width of 8, increasing with more bars
    fig_height = 6 if bar_orientation == 'vertical' else max(6, num_bars / 5)  # Adjust height for horizontal bars
    fig, ax = plt.subplots(figsize=(fig_width, fig_height*1.15))

    if bar_orientation == 'horizontal':
        ax.barh(data_type, total_votes, color=colors)
        ax.set_xlabel('Total Votes', fontsize=12)
        ax.set_ylabel('Candidate', fontsize=12)
    else:
        ax.bar(data_type, total_votes, color=colors)
        ax.set_ylabel('Total Votes', fontsize=12)
        ax.set_xlabel('Candidate', fontsize=12)
        ax.set_xticks(range(num_bars))  # Set tick positions
        ax.set_xticklabels(data_type, rotation=45, ha='right', fontsize=max(8, 12 - num_bars // 10))  # Adjust font size

    ax.set_title('Vote Counts per Candidate', fontsize=14)

    # Add value annotations on top of bars
    if add_values:
        if bar_orientation == 'horizontal':
            for i, v in enumerate(total_votes):
                ax.text(v + max(total_votes) * 0.01, i, str(v), va='center', ha='left', fontsize=10)
        else:
            for i, v in enumerate(total_votes):
                ax.text(i, v + max(total_votes) * 0.01, str(v), ha='center', va='bottom', fontsize=10)

    # Adjust layout to avoid overlap
    plt.subplots_adjust(bottom=0.3 if bar_orientation == 'vertical' else 0.1,
                        top=0.9,
                        left=0.2 if bar_orientation == 'horizontal' else 0.1,
                        right=0.95)
    plt.tight_layout()  # Ensure tight layout
    return plt


# Improved donut chart function with color palette option and validation
def plot_donut_chart(data: pd.DataFrame, title='Donut Chart', chart_type='candidate', color_map='Set3', label_shift=(0, 0)):
    if data.empty:
        print("No data available.")
        return None

    if chart_type == 'candidate':
        labels = list(data['candi_name'])
    elif chart_type == 'gender':
        labels = list(data['gender'])
    else:
        print("Invalid chart type. Use 'candidate' or 'gender'.")
        return None

    sizes = list(data['Total_Votes'])
    num_labels = len(labels)  # Number of labels to dynamically adjust the layout
    colors = colormaps[color_map](np.linspace(0, 1, num_labels))

    # Dynamically adjust figure size based on the number of labels
    fig_width = max(6, num_labels / 3)  # Base width of 6, increasing with more labels
    fig, ax = plt.subplots(figsize=(fig_width, fig_width))

    wedges, texts, autotexts = ax.pie(
        sizes,
        labels=labels,
        autopct='%1.1f%%',
        startangle=110,
        colors=colors,
        wedgeprops={'width': 0.4}
    )
    ax.set_facecolor('#A9A9A9')
    ax.axis('equal')  # Ensure the pie is drawn as a circle
    plt.title(title, fontsize=16)

    # Dynamically adjust the text inside the donut
    for i, autotext in enumerate(autotexts):
        angle = (wedges[i].theta2 + wedges[i].theta1) / 2  # Mid angle of the wedge
        distance = 0.78  # Default distance for text from the center of the circle
        shift_x, shift_y = label_shift  # X and Y shift values for fine control over positioning

        # Convert polar to cartesian coordinates (radius, angle) to create circular positioning
        x = np.cos(np.radians(angle)) * distance + shift_x
        y = np.sin(np.radians(angle)) * distance + shift_y

        # Move the autotext to a circular path
        autotext.set_position((x, y))
        autotext.set_fontsize(max(6, 12 - num_labels // 10))  # Adjust font size dynamically
        autotext.set_fontweight('bold')
        autotext.set_color('black')

        # Optionally, adjust alignment for better text positioning
        autotext.set_horizontalalignment('center')
        autotext.set_verticalalignment('center')

    # Dynamically adjust legend placement
    legend_cols = min(4, num_labels)  # Split legend into columns if too many labels
    ax.legend(
        wedges, labels,
        title="Candidates",
        loc="upper center",
        bbox_to_anchor=(.5, 1.3),
        ncol=legend_cols
    )

    # Adjust layout to avoid overlap
    plt.subplots_adjust(left=0.1, right=0.8, bottom=0.1, top=0.9)  # Dynamic margins
    plt.tight_layout()  # Ensure tight layout
    return fig

# Function to split a dataframe into chunks for pagination
@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    """Split the input DataFrame into batches of a given size."""
    return [input_df.iloc[i: i + rows] for i in range(0, len(input_df), rows)]

# Function to paginate a table
def paginate_table(table_data):
    # Top menu for sorting options
    with st.container():
        st.markdown("### Table Options")
        top_menu = st.columns([2, 2, 2])

        with top_menu[0]:
            sort = st.radio(
                "Sort Data",
                options=["No", "Yes"],
                horizontal=True,
                index=0,
                help="Enable sorting to arrange data by selected column."
            )

        if sort == "Yes":
            with top_menu[1]:
                sort_field = st.selectbox(
                    "Sort By",
                    options=table_data.columns,
                    help="Select the column to sort the data by."
                )
            with top_menu[2]:
                sort_direction = st.radio(
                    "Direction",
                    options=["⬆️ Ascending", "⬇️ Descending"],
                    horizontal=True,
                    help="Choose the sort direction.",
                )
            # Apply sorting
            try:
                table_data = table_data.sort_values(
                    by=sort_field,
                    ascending=(sort_direction == "⬆️ Ascending"),
                    ignore_index=True,
                )
                st.success(f"Data sorted by **{sort_field}** in **{sort_direction}** order.")
            except Exception as e:
                st.error(f"Error sorting data: {e}")

    # Pagination container
    pagination = st.container()

    # Bottom menu for pagination options
    with st.container():
        st.markdown("### Pagination")
        bottom_menu = st.columns((3, 1, 1))

        with bottom_menu[2]:
            batch_size = st.selectbox(
                "Page Size",
                options=[10, 25, 50, 100],
                index=1,
                help="Select the number of rows to display per page."
            )

        with bottom_menu[1]:
            # Ensure total pages are recalculated correctly
            total_pages = max(1, -(-len(table_data) // batch_size))  # Ceiling division
            current_page = st.number_input(
                "Page",
                min_value=1,
                max_value=total_pages,
                value=1,
                step=1,
                help="Navigate between pages."
            )

        with bottom_menu[0]:
            st.markdown(f"Page **{current_page}** of **{total_pages}**")

    # Split the DataFrame into pages
    pages = split_frame(table_data, batch_size)

    # Display the current page
    if len(pages) >= current_page:
        pagination.dataframe(data=pages[current_page - 1], use_container_width=True)
    else:
        st.error("No data to display on this page. Adjust the page size or sorting options.")


# Example usage:
# paginate_table(your_dataframe_here)



# Function to update data displayed on the dashboard
def update_data():
    # Placeholder to display last refresh time
    last_refresh = st.empty()
    status_message = st.empty()  # Ensure it's defined before try-except
    try:
        last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Fetch voting statistics
        voters_count, candidates_count = fetch_voting_stats()

        # Display total voters and candidates metrics
        st.markdown("<hr style='margin: 0; border: 1px solid #ffffff;' />",unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                        f"""
                        <div style='text-align:center; color: #ffff;'>
                            <h3 style='margin: 0;'>Total Voters<br><strong>{voters_count}</strong></h3>
                            <hr style='margin: 0; border: 1px solid #ffffff;' />
                        </div>""",unsafe_allow_html=True)
            # st.markdown("""---""")
        with col2:
            st.markdown(
                            f"""
                            <div style='text-align:center; color: #ffff;'>
                                <h3 style='margin: 0;'>Total Voters<br><strong>{candidates_count}</strong></h3>
                                <hr style='margin: 0; border: 1px solid #ffffff;' />
                            </div>""",unsafe_allow_html=True)

        # Show waiting message before fetching data
        # st.info("⏳ Waiting for data processing...")

        #    st.markdown("""---""")
        # Fetch data from Kafka on aggregated votes per candidate
        consumer = create_kafka_consumer("agg_totalvotes")
        data = fetch_data_from_kafka(consumer)
        results = pd.DataFrame(data)


        # Check if results are empty
        # if results.empty:
        #     st.warning("⚠ No data available. Waiting for votes to be processed...")
        #     return

        # # Check if 'candi_id' column exists before grouping
        # if 'candi_id' not in results.columns:
        #     st.error("❌ Data error: Missing 'candi_id' column. Please check your database.")
        #     return

        # # Show message before processing leading candidate
        # st.info("🔍 Processing votes to find the leading candidate...")


        # Identify the leading candidate
        results = results.loc[results.groupby('candi_id')['Total_Votes'].idxmax()]
        leading_candidate = results.loc[results['Total_Votes'].idxmax()]

        # Display leading candidate information
        # st.markdown("""---""")
        st.markdown("<h1 style='text-align: center;'>Leading Candidate</h1>", unsafe_allow_html=True)
        # st.header('Leading Candidate')
        col1, col2 = st.columns(2)
        with col1:
            # st.image(leading_candidate['photo_url'], width=200)
            st.image(leading_candidate['photo_url'], width=220, caption=f"{leading_candidate['candi_name']}")

        with col2:
            st.markdown("<h2 style='text-align: center; color: #FFFF; margin: 0;'>Candidate Name</h2>",unsafe_allow_html=True)
            st.markdown(f"<h3 style='text-align: center; color: #FFFF; margin: 0;'><strong>{leading_candidate['candi_name']}</strong></h3>",unsafe_allow_html=True)
            st.markdown(f"<h3 style='text-align: center; color: #FFFF;'>The Leading Party<br><strong>{leading_candidate['party_affiliation']}</strong></h3>",unsafe_allow_html=True)        # st.subheader(f"The Leading Party:\n{leading_candidate['party_affiliation']}")
            st.markdown(f"<h4 style='text-align: center; color: #FFF15;'>Total Votes:<br><strong>{leading_candidate['Total_Votes']}</strong></h4>",unsafe_allow_html=True)        # st.subheader(f"Total Vote: {leading_candidate['Total_Votes']}")

        # Display statistics and visualizations
        st.markdown("<hr style='margin: 0; border: 1px solid #ffffff;' />",unsafe_allow_html=True)
        st.header('Statistics')
        results = results[['candi_id', 'candi_name', 'party_affiliation', 'Total_Votes']].reset_index(drop=True)

        # results = results.reset_index(drop=True)
        col1, col2 = st.columns(2)
        # Display bar chart and donut chart
        with col1:
            bar_fig = plot_colored_bar_chart(results,bar_orientation='vertical',color_map='Set3')
            st.pyplot(bar_fig)

        with col2:
            donut_fig = plot_donut_chart(results, title='Vote Distribution')
            st.pyplot(donut_fig)

        # Display table with candidate statistics
        st.markdown("### Election Results")
        st.markdown("This table shows the total votes received by each candidate.")
        results = results.sort_values(by='Total_Votes', ascending=False).reset_index(drop=True)
        results.index = results.index +1
        st.dataframe(results, use_container_width=True)

        # Fetch data from Kafka on aggregated turnout by location
        # st.info("📍 Fetching location-based voter data...")

        location_consumer = create_kafka_consumer("agg_vote_count")
        location_data = fetch_data_from_kafka(location_consumer)
        location_result = pd.DataFrame(location_data)

        # Identify locations with maximum turnout
        location_result = location_result.loc[location_result.groupby('address[state]')['vote_count'].idxmax()].reset_index(drop=True)
        # location_result = location_result.reset_index(drop=True)

        # Display location-based voter information with pagination
        st.markdown("<hr style='margin: 0; border: 1px solid #ffffff;' />",unsafe_allow_html=True)
        st.header("Locations Voters")
        paginate_table(location_result)

        # Update the last refresh time
        st.session_state['last_update'] = time.time()

     # Clear any previous error messages
        status_message.empty()

    except KeyError as e:
        status_message.warning("⏳ Data is coming or processing... Please wait.")

    except Exception as e:
        status_message.error(f"⚠️ An unexpected error occurred: {e}")

    # Sidebar layout
    # def sidebar():
    #     # Initialize last update time if not present in session state
    #     if st.session_state.get('last_update') is None:
    #         st.session_state['last_update'] = time.time()

    #     # Slider to control refresh interval
    #     refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    #     st_autorefresh(interval=refresh_interval * 1000, key="auto")

    #     # Button to manually refresh data
    #     if st.sidebar.button('Refresh Data'):
    #         update_data()

if __name__=="__main__":

    # Title of the Streamlit dashboard
    st.markdown( """
                    <div style='background-color: #282c34; padding: 20px; border-radius: 10px;'>
                    <h1 style='text-align: center; color: #4CAF50;'>Real-time UK-Election Dashboard</h1>
                    <h3 style='text-align: center; color: #FFFFFF;'>Track the latest election results & statistics!</h3>
                    </div>""",unsafe_allow_html=True)
    # topic_name = 'agg_totalvotes'

    # Display sidebar
    # sidebar()

    # Automatic refresh logic in main area
    st_autorefresh(interval=10000, key="auto_refresh")  # Refresh every 10 seconds
    # Update and display data on the dashboard
    update_data()