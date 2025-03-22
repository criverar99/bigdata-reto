import streamlit as st
import pandas as pd

csv_routes = {
    "sales": "https://raw.githubusercontent.com/criverar99/bigdata-reto/refs/heads/main/Datasets/sales.csv",
    "products": "https://raw.githubusercontent.com/criverar99/bigdata-reto/refs/heads/main/Datasets/products.csv",
    "stores": "https://raw.githubusercontent.com/criverar99/bigdata-reto/refs/heads/main/Datasets/stores.csv",
}

@st.cache_data
def cargar_datos():
    df_sales = pd.read_csv(csv_routes["sales"])
    df_products = pd.read_csv(csv_routes["products"])
    df_stores = pd.read_csv(csv_routes["stores"])
    df_sales = df_sales.merge(df_products, on="Product_ID", how="left")
    df_sales = df_sales.merge(df_stores, on="Store_ID", how="left")
    df_sales["Units"] = df_sales["Units"].astype(float)
    df_sales["Product_Price_Float"] = df_sales["Product_Price"].replace('[\$,]', '', regex=True).astype(float)
    df_sales["Product_Cost_Float"] = df_sales["Product_Cost"].replace('[\$,]', '', regex=True).astype(float)
    df_sales["Total_Sales_Value"] = df_sales["Units"] * df_sales["Product_Price_Float"]
    df_sales["Total_Cost"] = df_sales["Units"] * df_sales["Product_Cost_Float"]
    df_sales["Total_Profit"] = df_sales["Total_Sales_Value"] - df_sales["Total_Cost"]
    df_sales["Total_Profit_Dollar"] = df_sales["Total_Profit"].apply(lambda x: f"${x:,.2f}")
    return df_sales

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
    payload = {
      "event_type": job,

      "client_payload" : {
        "codeurl" : codeurl,
        "dataseturl" : dataseturl
      }
    }

    headers = {
      'Authorization': 'Bearer ' + token,
      'Accept': 'application/vnd.github.v3+json',
      'Content-type': 'application/json'
    }

    st.write(url)
    st.write(payload)
    st.write(headers)

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    # Display the response in the app
    st.write(response)

def get_spark_results(url_results):
    # Make the GET request
    response = requests.get(url_results)
    st.write(response)
    # Parse the response
    if response.status_code == 200:
        data = response.json()
        # Display the response in the app
        st.write(data)

df = cargar_datos()

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["Dashboard", "Update Data", "MongoDB", "PostgreSQL"])

with tab1:
    # Título del Dashboard
    st.title("Mexican Toys Sales Dashboard")

    # Sidebar para los filtros
    st.sidebar.header("Filtros")
    mostrar_todos = st.sidebar.checkbox("Mostrar todas las ventas")
    # Filtro por producto
    productos = ["Todos los productos"] + list(df['Product_Name'].unique())
    producto_seleccionado = st.sidebar.selectbox("Seleccionar Producto", options=productos)

    # Filtro por tipo de producto
    product_type = ["Todos los tipos de productos"] + list(df['Product_Category'].unique())
    product_type_seleccionado = st.sidebar.selectbox("Seleccionar Tipo de Producto", options=product_type)

    # Filtro por tienda
    tiendas = ["Todas las tiendas"] + list(df['Store_Name'].unique())
    tienda_seleccionada = st.sidebar.selectbox("Seleccionar Tienda", options=tiendas)

    if st.sidebar.button("Filtrar"):
        if not mostrar_todos:
            if producto_seleccionado != "Todos los productos":
                df = df[df['Product_Name'] == producto_seleccionado]
            if product_type_seleccionado != "Todos los tipos de productos":
                df = df[df['Product_Category'] == product_type_seleccionado]
            if tienda_seleccionada != "Todas las tiendas":
                df = df[df['Store_Name'] == tienda_seleccionada]

    # Mostrar el número total de filmes
    st.write(f"Total ventas: {len(df)}")

    # Mostrar la tabla filtrada
    st.dataframe(df.drop(columns=["Product_ID", "Store_ID", "Product_Price_Float", "Product_Cost_Float", "Total_Sales_Value", "Total_Cost", "Total_Profit"]))

    # Mostrar total de ventas y de ganancias
    total_ventas = df["Total_Sales_Value"].sum()
    total_ganancias = df["Total_Profit"].sum()
    st.write(f"Total de ventas: ${total_ventas:,.2f}")
    st.write(f"Total de ganancias: ${total_ganancias:,.2f}")

with tab2:
    st.title("Activando GitHub Actions")
    st.header("spark-submit Job")

    github_user  =  st.text_input('Github user', value='criverar99')
    github_repo  =  st.text_input('Github repo', value='bigdata-reto')
    spark_job    =  st.text_input('Spark job', value='spark')
    github_token =  st.text_input('Github token', value='***')
    codeurl =  st.text_input('Github code url', value='https://raw.githubusercontent….')
    dataseturl =  st.text_input('Github token', value='https://raw.githubusercontent….')

    if st.button("Activar job"):
        post_spark_job(github_user, github_repo, spark_job, github_token, codeurl, dataseturl)

    st.header("Resultados")

    url_results=  st.text_input('URL de los resultados', value='https://raw.githubusercontent….')

    if st.button("Obtener resultados"):
        get_spark_results(url_results)



with tab3:
    st.write("This is to display the MongoDB data")

with tab4:
    st.write("This is to display the PostgreSQL data")