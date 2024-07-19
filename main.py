from pyspark.sql import SparkSession

def main():
    # Создаем SparkSession
    spark = SparkSession.builder.appName("ProductCategoryApp").getOrCreate()

    # Загружаем данные из CSV файлов
    products_df = spark.read.csv('data/products.csv', header=True, inferSchema=True)
    categories_df = spark.read.csv('data/categories.csv', header=True, inferSchema=True)
    product_category_rel_df = spark.read.csv('data/product_category_rel.csv', header=True, inferSchema=True)

    # Join products_df с product_category_rel_df
    product_category_df = products_df.join(product_category_rel_df, on="product_id")

    # Join product_category_df с categories_df
    product_category_df = product_category_df.join(categories_df, on="category_id")

    # Выбираем необходимые столбцы
    product_category_pairs_df = product_category_df.select("product_name", "category_name")

    # Left anti join для поиска продуктов без категорий
    products_without_categories_df = products_df.join(product_category_rel_df, on="product_id", how="left_anti")

    # Выбираем столбец product_name
    products_without_categories_df = products_without_categories_df.select("product_name")

    # Показать результат
    print("Пары 'Имя продукта – Имя категории':")
    product_category_pairs_df.show()

    print("Продукты без категорий:")
    products_without_categories_df.show()

    # Останавливаем SparkSession
    spark.stop()

if __name__ == '__main__':
    main()
