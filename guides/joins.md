# Common Database Join Patterns

This document outlines common patterns for joining tables based on their relationships. These patterns help model complex data structures in a relational database.

## Enhanced Join Types

Selecto now supports advanced join types beyond basic LEFT/INNER joins. For detailed documentation on enhanced joins including self-joins, lateral joins, cross joins, full outer joins, and conditional joins, see [Enhanced Joins Guide](../docs/enhanced_joins.md).

## Traditional Join Patterns

## Star Schema

A star schema is common in data warehousing and business intelligence. It features a central "fact" table connected to multiple "dimension" tables.

-   **Fact Table:** Contains quantitative data (measurements, metrics) and foreign keys to the dimension tables. Examples: `sales`, `orders`, `page_views`.
-   **Dimension Tables:** Contain descriptive attributes that qualify the facts. Examples: `customers`, `products`, `dates`.

This "star" shape simplifies queries for reporting and aggregation.

**Example:**

A `sales` fact table might be joined with `customers`, `products`, and `time_dim` dimension tables to analyze sales by customer, product, and date.

```
      +-------------+
      |  customers  |
      +-------------+
            |
+----------+v----------+      +------------+
|      sales_facts    +------>|  products  |
+---------------------+      +------------+
            |
      +-------------+
      |    dates    |
      +-------------+
```

## Snowflake Schema

A snowflake schema is an extension of a star schema where dimension tables are normalized into multiple related tables. This creates a more complex, snowflake-like shape.

**Example:**

In a star schema, a `products` dimension might contain brand and category information. In a snowflake schema, the `products` table would have a foreign key to a `brands` table and another to a `categories` table.

```
+------------+      +----------+
| categories |<-----+ products |
+------------+      +----------+
                        |
+----------+      +-----+-----+
|  brands  |<-----+ sales_facts |
+----------+      +-----------+
```

## Tagging / Many-to-Many

This pattern is used to associate items from two tables where an item from one table can be related to many items in the other, and vice-versa. A third "join" or "link" table is used to manage the relationship.

This is common for:
-   Assigning multiple tags to a blog post.
-   Associating users with multiple groups.
-   Linking products to multiple categories.

**Example:**

To associate `posts` with `tags`, a `post_tags` table is created. It contains a `post_id` and a `tag_id`.

```sql
SELECT posts.title, tags.name
FROM posts
INNER JOIN post_tags ON posts.id = post_tags.post_id
INNER JOIN tags ON post_tags.tag_id = tags.id;
```

```
+-------+       +-----------+       +------+
| posts |<----->| post_tags |<----->| tags |
+-------+       +-----------+       +------+
```

## Hierarchical / Adjacency List

This pattern is used to represent tree-like structures, such as organizational charts, product categories, or threaded comments. A table references itself to define parent-child relationships.

**Example:**

An `employees` table with a `manager_id` column that is a foreign key to the same `employees` table's `id` column.

```sql
SELECT
    e.name AS employee_name,
    m.name AS manager_name
FROM
    employees e
LEFT JOIN
    employees m ON e.manager_id = m.id;
```

This allows you to represent a hierarchy of employees and their managers within a single table.

## Multi-Level Schema Examples

### Product Catalog Schema

This example combines one-to-many and many-to-many relationships to create a product catalog.

-   A `product` belongs to one `category`.
-   A `product` can have many `tags`.
-   A `tag` can be applied to many `products`.

```
+------------+       +----------+       +--------------+       +------+
| categories |<------| products |<----->| product_tags |<----->| tags |
+------------+       +----------+       +--------------+       +------+
```

**Example Query:** Find all products in the 'Electronics' category, and list their tags.

```sql
SELECT
    p.name AS product_name,
    c.name AS category_name,
    t.name AS tag_name
FROM
    products p
JOIN
    categories c ON p.category_id = c.id
LEFT JOIN
    product_tags pt ON p.id = pt.product_id
LEFT JOIN
    tags t ON pt.tag_id = t.id
WHERE
    c.name = 'Electronics';
```

### Invoicing Schema for Product Catalog

This schema builds on the product catalog to create invoices. It involves multiple one-to-many and many-to-many relationships.

-   An `invoice` belongs to a `customer`.
-   An `invoice` has many `invoice_items`.
-   An `invoice_item` links an `invoice` to a `product` and includes the quantity and price at the time of purchase.

```
+-----------+       +----------+       +---------------+       +----------+
| customers |<------| invoices |<------| invoice_items |----->| products |
+-----------+       +----------+       +---------------+       +----------+
```

**Example Query:** Get all items for a specific invoice, including product name and subtotal.

```sql
SELECT
    i.id AS invoice_id,
    p.name AS product_name,
    ii.quantity,
    ii.unit_price,
    (ii.quantity * ii.unit_price) AS subtotal
FROM
    invoices i
JOIN
    invoice_items ii ON i.id = ii.invoice_id
JOIN
    products p ON ii.product_id = p.id
WHERE
    i.id = 12345;
```

## Polymorphic Associations

This pattern allows a model to belong to more than one other model on a single association. For example, a `comments` table could store comments for both `articles` and `videos`. This is achieved by using two columns to identify the parent record: a foreign key and a type column.

-   `commentable_id`: The ID of the parent record (e.g., the ID of the article or video).
-   `commentable_type`: The name of the parent table (e.g., 'articles' or 'videos').

**Example:**

To get all comments for a specific article:
```sql
SELECT *
FROM comments
WHERE commentable_id = 1 AND commentable_type = 'articles';
```

## Alternative Hierarchical Patterns

While the Adjacency List is simple, it can be inefficient for querying deep hierarchies. Here are two alternatives:

### Materialized Path

This pattern stores the entire path from the root to the current node in a single column (e.g., a string like `1/2/5/`). This makes it very efficient to query for ancestors or descendants.

**Example:** To find all descendants of a node with path `1/2/`:
```sql
SELECT *
FROM categories
WHERE path LIKE '1/2/%';
```

### Closure Table

This pattern uses a separate table to store all ancestor-descendant relationships. The table has at least three columns: `ancestor_id`, `descendant_id`, and `depth`. This provides great flexibility for complex hierarchy queries at the cost of more storage space.

**Example:** To find all descendants of node `2`:
```sql
SELECT c.*
FROM categories c
JOIN category_paths cp ON c.id = cp.descendant_id
WHERE cp.ancestor_id = 2;
```

### Nested Set

The Nested Set model is another way to represent hierarchies. Each node is stored with `lft` and `rgt` values, which represent its position in a pre-order traversal of the tree. A node's descendants are all the nodes with `lft` and `rgt` values that are between its own `lft` and `rgt`.

**Example:** To find all descendants of a "Clothing" category:
```sql
SELECT node.*
FROM categories AS node, categories AS parent
WHERE parent.name = 'Clothing' AND node.lft BETWEEN parent.lft AND parent.rgt;
```
This pattern is very efficient for reads, but updates can be slow as they may require recalculating `lft` and `rgt` values for many nodes.

## Entity-Attribute-Value (EAV)

EAV is a flexible pattern for when an entity's attributes are not known in advance or vary greatly between entities. Instead of columns for each attribute, rows are used.

-   **Entity Table:** The items themselves (e.g., `products`).
-   **Attribute Table:** The possible attributes (e.g., `color`, `weight`, `voltage`).
-   **Value Table:** The specific value of an attribute for an entity.

**Example:** To store product specifications:
```sql
-- Find the voltage of product with ID 101
SELECT value
FROM product_attributes
WHERE product_id = 101 AND attribute_id = (SELECT id FROM attributes WHERE name = 'voltage');
```
This pattern is very flexible but can lead to complex and less performant queries.

## Slowly Changing Dimensions (SCD)

In data warehousing, this pattern manages the history of data in dimension tables.

-   **Type 1 (Overwrite):** The old value is simply replaced with the new one. No history is kept.
-   **Type 2 (Add New Row):** A new row is added for the new state of the dimension, and existing rows are preserved. Columns like `start_date`, `end_date`, and `is_current` are used to track the active record.

**Example (Type 2):** When a customer moves, a new address record is created.

```sql
-- Find the customer's current address
SELECT *
FROM customer_addresses
WHERE customer_id = 5 AND is_current = TRUE;
```
This allows for accurate historical reporting.

## Obscure Join Patterns

### Cross Join

A `CROSS JOIN` creates a Cartesian product of two tables, returning all possible combinations of rows. It's useful for generating a complete set of pairings, but can produce very large result sets and should be used with caution.

**Example:** To get all possible pairings of `employees` and `projects`:
```sql
SELECT e.name, p.name
FROM employees e
CROSS JOIN projects p;
```

### Full Outer Join

A `FULL OUTER JOIN` returns all records when there is a match in either the left or the right table. It's useful for data reconciliation, as it can show records that exist in one table but not the other.

**Example:** To find all `employees` and the `department` they might belong to, showing employees without a department and departments without employees:
```sql
SELECT e.name, d.name
FROM employees e
FULL OUTER JOIN departments d ON e.department_id = d.id;
```

### Lateral Join

A `LATERAL JOIN` allows a subquery in the `FROM` clause to reference columns from a preceding table. This is powerful for "top-N-per-category" problems.

**Example:** To get the three most recent posts for each `user`:
```sql
SELECT u.name, p.title, p.created_at
FROM users u,
LATERAL (
    SELECT *
    FROM posts
    WHERE posts.user_id = u.id
    ORDER BY posts.created_at DESC
    LIMIT 3
) p;
```
This is supported in PostgreSQL and other modern databases. The `CROSS APPLY` and `OUTER APPLY` keywords in SQL Server provide similar functionality.

### Graph Traversal (Recursive CTEs)

For graph-like data structures (e.g., social networks, organizational charts), you can use a recursive Common Table Expression (CTE) to traverse the relationships.

**Example:** To find all employees who report up to a specific manager:
```sql
WITH RECURSIVE subordinates AS (
    SELECT id, name, manager_id
    FROM employees
    WHERE id = 1 -- The manager's ID
    UNION
    SELECT e.id, e.name, e.manager_id
    FROM employees e
    INNER JOIN subordinates s ON s.id = e.manager_id
)
SELECT * FROM subordinates;
```

### Bitmasking

This pattern uses the bits of an integer to store a set of boolean flags. It is very storage-efficient but can make queries more complex.

**Example:** To store user permissions, where `1` is read, `2` is write, and `4` is execute. A user with write and execute permissions would have a permission value of `6` (`2 | 4`).

To find all users with write permission:
```sql
SELECT *
FROM users
WHERE (permissions & 2) > 0; -- Using the bitwise AND operator
```
This pattern is fast but not easily extensible. Adding a new permission might require changing the meaning of all existing bitmasks.