CREATE TABLE `warehouse_db` (
  `warehouse` varchar(255),
  `product` varchar(255),
  `categories` varchar(255),
  `supplier` varchar(255),
  `stock_id` integer,
  `purchase_order_id` integer,
  `sales_order_id` integer,
  PRIMARY KEY (`stock_id`, `purchase_order_id`, `sales_order_id`)
);

CREATE TABLE `stock_db` (
  `stock` integer,
  `stock_movement` varchar(255)
);

CREATE TABLE `purchase_order_db` (
  `purchase_order` integer,
  `purchase_order_detail` varchar(255)
);

CREATE TABLE `sales_order_db` (
  `sales_order` integer,
  `sales_order_detail` varchar(255)
);

ALTER TABLE `stock_db` ADD FOREIGN KEY (`stock`) REFERENCES `warehouse_db` (`stock_id`);

ALTER TABLE `purchase_order_db` ADD FOREIGN KEY (`purchase_order`) REFERENCES `warehouse_db` (`purchase_order_id`);

ALTER TABLE `sales_order_db` ADD FOREIGN KEY (`sales_order`) REFERENCES `warehouse_db` (`sales_order_id`);
