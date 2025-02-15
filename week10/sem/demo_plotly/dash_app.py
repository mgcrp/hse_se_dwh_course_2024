# ----------------- ИМПОРТЫ ----------------

import dash
import psycopg2
import pandas as pd
import sqlalchemy as sa
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html

from plotly.subplots import make_subplots
from dash.dependencies import Input, Output


# ---------- ПЕРЕМЕННЫЕ/КОНСТАНТЫ ----------

db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}

# ----------------- ФУНКЦИИ ----------------

pass

# ------------------- КОД ------------------

app = dash.Dash(__name__)
app.title = "HSE FTDA DWH Sample App"

conn = psycopg2.connect(**db_params)
cur = conn.cursor()
cur.execute("SELECT DISTINCT category_name FROM business.categories;")
dropdown_options = [{'label': row[0], 'value': row[0]} for row in cur.fetchall()]
conn.close()

app.layout = html.Div([
    html.H1('HSE FTDA DWH Sample Dashboard', style={'textAlign': 'center', 'color': 'RebeccaPurple'}),
    dcc.Dropdown(
        id='sentiment-filter', options=dropdown_options, value=[], multi=True,
        placeholder='Filter by category', style={'marginBottom': '10px'}
    ),
    html.Div(
        dcc.Input(
            id='keyword-search', type='text', placeholder='Search products by name', 
            style={'width': '98%', 'padding': '10px', 'margin': '10px 0', 'borderRadius': '5px', 'border': '1px solid #ddd'}
        )
    ),
    dcc.Graph(id='product-analysis-plot'),
], style={'padding': '20px'})

engine = sa.create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:5432/{db_params["dbname"]}')

@app.callback(
    Output('product-analysis-plot', 'figure'),
    [Input('sentiment-filter', 'value'),
     Input('keyword-search', 'value')]
)
def update_figure(selected_sentiments, search_keyword):
    # Prepare the query with dynamic filtering based on user input
    query = """
    select
        date(purchase_date) as purchase_date, manufacturer_name, category_name, ps.product_name as product_name,
        sum(product_count) as cnt,
        sum(product_count * product_price) as gmv
    from business.purchase_items as pi
    join business.purchases as ps
        on pi.purchase_id = ps.purchase_id
    join business.products as p
        on p.product_id = pi.product_id
    join business.categories as c
        on p.category_id = c.category_id
    join business.manufacturers as m
        on p.manufacturer_id = m.manufacturer_id
    where 1=1
    """
    if selected_sentiments:
        query += " AND category_name IN ('{filter}')".format(
            filter = "', '".join(selected_sentiments)
        )
    if search_keyword:
        keyword_filter = f'%%{search_keyword}%%'
        query += f" AND (ps.product_name LIKE '{keyword_filter}')"
    
    query += " group by 1, 2, 3, 4;"
    
    # Execute the query
    print(query)
    df_filtered = pd.read_sql_query(query, engine)

    # Create the subplot structure with increased size
    fig = make_subplots(
        rows=2, cols=2,
        specs=[[{"type": "bar"}, {"type": "pie"}], [{"colspan": 2}, None]],
        subplot_titles=('Sales by Date', 'GMV by category', 'Product Count Distribution'),
        vertical_spacing=0.18, horizontal_spacing=0.1
    )

    # Check if the filtered DataFrame is not empty
    if df_filtered.empty:
        fig.add_annotation(
            text="No data matches the selected filters.",
            x=0.5, y=0.5, showarrow=False, font_size=16, xref="paper", yref="paper"
        )
    else:
        # Line Chart
        df_dates = df_filtered.groupby('purchase_date').gmv.sum()
        fig.add_trace(
            go.Line(x=df_dates.index, y=df_dates.values, name='Sales by Date', marker_color='royalblue'),
            row=1, col=1
        )

        df_categories = df_filtered.groupby('category_name').gmv.sum()
        # Pie Chart
        fig.add_trace(
            go.Pie(labels=df_categories.index, values=df_categories.values, name='GMV by category'),
            row=1, col=2
        )

        query_hist = """
        select product_count
        from business.purchase_items as pi
        join business.products as p
            on p.product_id = pi.product_id
        join business.categories as c
            on p.category_id = c.category_id
        where 1=1
        """

        if selected_sentiments:
            query_hist += " AND category_name IN ('{filter}')".format(
                filter = "', '".join(selected_sentiments)
            )
        if search_keyword:
            keyword_filter = f'%%{search_keyword}%%'
            query_hist += f" AND (ps.product_name LIKE '{keyword_filter}')"

        df_hist = pd.read_sql_query(query_hist, engine)
        # Histogram for Product Count Distribution
        fig.add_trace(
            go.Histogram(x=df_hist['product_count'], name='Product Count Distribution'),
            row=2, col=1
        )
    
    fig.update_layout(
        height=800,
        showlegend=True,
        title_text="HSE FTDA DWH Sample Dashboard"
    )

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)