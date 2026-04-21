CREATE OR REFRESH MATERIALIZED VIEW current_books
AS select book_id, title, author, price
from books_silver
where `__END_AT` is null;

create or refresh streaming table books_sales(
  CONSTRAINT valid_book expect (b.book.subtotal = b.book.quantity * c.price) on violation drop row,
  CONSTRAINT valid_total expect (total between 0 and 1000000) on violation fail update,
  constraint valid_date expect (order_timestamp <= current_date() and year(order_timestamp)>=2020)
) as select * 
from stream(orders_silver) as o,
      lateral explode(o.books) as b(book)
      inner join current_books c 
      on b.book.book_id = c.book_id;


create or refresh materialized view author_stats
as
select 
author, 
window.start as window_start,
window.end as window_end,
count("order_id") as orders_count,
avg("quantity") as avg_quantity
from books_sales
group by author, 
window(order_timestamp, '5 minutes', '5 minutes', '2 minutes')
order by
window.start;
