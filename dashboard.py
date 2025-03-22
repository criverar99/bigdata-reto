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
    st.write("This is to display the form for the GitHub Actions")

with tab3:
    st.write("This is to display the MongoDB data")

with tab4:
    st.write("This is to display the PostgreSQL data")