using System.Data.Common;
using System.Globalization;
using Northwind.Services.Repositories;

namespace Northwind.Services.Ado.Repositories
{
    public sealed class OrderRepository : IOrderRepository
    {
        private readonly DbConnection context;

        public OrderRepository(DbProviderFactory dbProviderFactory, string connectionString)
        {
            if (dbProviderFactory == null)
            {
                throw new ArgumentNullException(nameof(dbProviderFactory));
            }

            this.context = dbProviderFactory.CreateConnection() ?? throw new InvalidOperationException("Failed to create a database connection.");
            this.context.ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public async Task<long> AddOrderAsync(Order order)
        {
            ValidateOrder(order);
            return await this.AddOrderInternalAsync(order);
        }

        public async Task<Order> GetOrderAsync(long orderId)
        {
            await this.context.OpenAsync();
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT * FROM Orders WHERE OrderID = @OrderID";
            command.Parameters.Add(CreateParameter(command, "@OrderID", orderId));

            Order? order = null;

            using (var reader = await command.ExecuteReaderAsync())
            {
                if (await reader.ReadAsync())
                {
                    order = new Order(orderId)
                    {
                        OrderDate = reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                        RequiredDate = reader.GetDateTime(reader.GetOrdinal("RequiredDate")),
                        ShippedDate = reader.GetDateTime(reader.GetOrdinal("ShippedDate")),
                        Freight = reader.GetDouble(reader.GetOrdinal("Freight")),
                        ShipName = reader.GetString(reader.GetOrdinal("ShipName")),
                        ShippingAddress = new ShippingAddress(
                            reader.GetString(reader.GetOrdinal("ShipAddress")),
                            reader.GetString(reader.GetOrdinal("ShipCity")),
                            reader.IsDBNull(reader.GetOrdinal("ShipRegion")) ? null : reader.GetString(reader.GetOrdinal("ShipRegion")),
                            reader.GetString(reader.GetOrdinal("ShipPostalCode")),
                            reader.GetString(reader.GetOrdinal("ShipCountry"))),

                        Customer = new Customer(new CustomerCode(reader.GetString(reader.GetOrdinal("CustomerID"))))
                        {
                            CompanyName = await this.GetCustomerCompanyNameAsync(reader.GetString(reader.GetOrdinal("CustomerID"))) ?? "Default Company Name",
                        },
                        Employee = await this.GetEmployeeAsync(reader.GetInt64(reader.GetOrdinal("EmployeeID"))),
                        Shipper = await this.GetShipperAsync(reader.GetInt64(reader.GetOrdinal("ShipVia"))),
                    };
                }
            }

            if (order == null)
            {
                throw new RepositoryException($"Order with ID {orderId} not found.");
            }

            await this.GetOrderDetailsAsync(order);

            await this.context.CloseAsync();
            return order;
        }

        public async Task<IList<Order>> GetOrdersAsync(int skip, int count)
        {
            CheckParameters(skip, count);
            return await this.GetOrdersInternalAsync(skip, count);
        }

        public async Task RemoveOrderAsync(long orderId)
        {
            await this.context.OpenAsync();
            var sqlTran = await this.context.BeginTransactionAsync();
            var command = this.context.CreateCommand();
            command.Transaction = sqlTran;

            try
            {
                // Delete from OrderDetails
                command.CommandText = "DELETE FROM OrderDetails WHERE OrderID = @OrderID";
                command.Parameters.Add(CreateParameter(command, "@OrderID", orderId));
                await command.ExecuteNonQueryAsync();

                // Delete from Orders
                command.CommandText = "DELETE FROM Orders WHERE OrderID = @OrderID";
                command.Parameters.Clear();
                command.Parameters.Add(CreateParameter(command, "@OrderID", orderId));
                await command.ExecuteNonQueryAsync();

                await sqlTran.CommitAsync();
            }
            catch (Exception ex)
            {
                await sqlTran.RollbackAsync();
                throw new RepositoryException(ex.Message, ex);
            }
            finally
            {
                await this.context.CloseAsync();
            }
        }

        public async Task UpdateOrderAsync(Order order)
        {
            ValidateOrder(order);
            await this.CheckAndUpdateOrderAsync(order);
        }

        private static void ValidateOrder(Order order)
        {
            if (order == null)
            {
                throw new ArgumentNullException(nameof(order));
            }
        }

        private static void CheckParameters(int skip, int count)
        {
            if (skip < 0 || count < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(count), "Skip or count parameters are out of range.");
            }
        }

        private static DbParameter CreateParameter(DbCommand command, string name, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            return parameter;
        }

        private async Task CheckAndUpdateOrderAsync(Order order)
        {
            await this.context.OpenAsync();
            var sqlTran = await this.context.BeginTransactionAsync();
            var command = this.context.CreateCommand();
            command.Transaction = sqlTran;
            try
            {
                // Update Order
                string typeDateTime = "yyyy-MM-dd HH:mm:ss";
                command.CommandText = "UPDATE Orders SET CustomerID = @CustomerID, EmployeeID = @EmployeeID, OrderDate = @OrderDate, RequiredDate = @RequiredDate, " +
                    "ShippedDate = @ShippedDate, ShipVia = @ShipVia, Freight = @Freight, ShipName = @ShipName, ShipAddress = @ShipAddress, ShipCity = @ShipCity, " +
                    "ShipRegion = @ShipRegion, ShipPostalCode = @ShipPostalCode, ShipCountry = @ShipCountry WHERE OrderID = @OrderID";
                command.Parameters.Add(CreateParameter(command, "@OrderID", order.Id));
                command.Parameters.Add(CreateParameter(command, "@CustomerID", order.Customer.Code.Code));
                command.Parameters.Add(CreateParameter(command, "@EmployeeID", order.Employee.Id));
                command.Parameters.Add(CreateParameter(command, "@OrderDate", order.OrderDate.ToString(typeDateTime, CultureInfo.InvariantCulture)));
                command.Parameters.Add(CreateParameter(command, "@RequiredDate", order.RequiredDate.ToString(typeDateTime, CultureInfo.InvariantCulture)));
                command.Parameters.Add(CreateParameter(command, "@ShippedDate", order.ShippedDate.ToString(typeDateTime, CultureInfo.InvariantCulture)));
                command.Parameters.Add(CreateParameter(command, "@ShipVia", order.Shipper.Id));
                command.Parameters.Add(CreateParameter(command, "@Freight", order.Freight));
                command.Parameters.Add(CreateParameter(command, "@ShipName", order.ShipName));
                command.Parameters.Add(CreateParameter(command, "@ShipAddress", order.ShippingAddress.Address.Replace("'", "`", StringComparison.Ordinal)));
                command.Parameters.Add(CreateParameter(command, "@ShipCity", order.ShippingAddress.City));
                command.Parameters.Add(CreateParameter(command, "@ShipRegion", order.ShippingAddress.Region));
                command.Parameters.Add(CreateParameter(command, "@ShipPostalCode", order.ShippingAddress.PostalCode));
                command.Parameters.Add(CreateParameter(command, "@ShipCountry", order.ShippingAddress.Country));
                await command.ExecuteNonQueryAsync();

                // Delete existing OrderDetails
                command.CommandText = "DELETE FROM OrderDetails WHERE OrderID = @OrderID";
                await command.ExecuteNonQueryAsync();

                // Add new OrderDetails
                foreach (var orderDetail in order.OrderDetails)
                {
                    command.CommandText = "INSERT INTO OrderDetails (OrderID, ProductID, UnitPrice, Quantity, Discount) VALUES (@OrderID, @ProductID, @UnitPrice, @Quantity, @Discount)";
                    command.Parameters.Clear();
                    command.Parameters.Add(CreateParameter(command, "@OrderID", order.Id));
                    command.Parameters.Add(CreateParameter(command, "@ProductID", orderDetail.Product.Id));
                    command.Parameters.Add(CreateParameter(command, "@UnitPrice", orderDetail.UnitPrice));
                    command.Parameters.Add(CreateParameter(command, "@Quantity", orderDetail.Quantity));
                    command.Parameters.Add(CreateParameter(command, "@Discount", orderDetail.Discount));
                    await command.ExecuteNonQueryAsync();
                }

                await sqlTran.CommitAsync();
                await this.context.CloseAsync();
            }
            catch (Exception ex)
            {
                await sqlTran.RollbackAsync();
                await this.context.CloseAsync();
                throw new RepositoryException(ex.Message, ex);
            }
        }

        private async Task<long> AddOrderInternalAsync(Order order)
        {
            await this.context.OpenAsync();
            var sqlTran = await this.context.BeginTransactionAsync();
            var command = this.context.CreateCommand();
            command.Transaction = sqlTran;

            try
            {
                // Add Order
                command.CommandText = "INSERT INTO Orders (CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ShipName, " +
                "ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry) VALUES (@CustomerID, @EmployeeID, @OrderDate, @RequiredDate, @ShippedDate, " +
                "@ShipVia, @Freight, @ShipName, @ShipAddress, @ShipCity, @ShipRegion, @ShipPostalCode, @ShipCountry)";
                command.Parameters.Add(CreateParameter(command, "@CustomerID", order.Customer.Code.Code));
                command.Parameters.Add(CreateParameter(command, "@EmployeeID", order.Employee.Id));
                command.Parameters.Add(CreateParameter(command, "@OrderDate", order.OrderDate));
                command.Parameters.Add(CreateParameter(command, "@RequiredDate", order.RequiredDate));
                command.Parameters.Add(CreateParameter(command, "@ShippedDate", order.ShippedDate));
                command.Parameters.Add(CreateParameter(command, "@ShipVia", order.Shipper.Id));
                command.Parameters.Add(CreateParameter(command, "@Freight", order.Freight));
                command.Parameters.Add(CreateParameter(command, "@ShipName", order.ShipName));
                command.Parameters.Add(CreateParameter(command, "@ShipAddress", order.ShippingAddress.Address.Replace("'", "`", StringComparison.Ordinal)));
                command.Parameters.Add(CreateParameter(command, "@ShipCity", order.ShippingAddress.City));
                command.Parameters.Add(CreateParameter(command, "@ShipRegion", order.ShippingAddress.Region));
                command.Parameters.Add(CreateParameter(command, "@ShipPostalCode", order.ShippingAddress.PostalCode));
                command.Parameters.Add(CreateParameter(command, "@ShipCountry", order.ShippingAddress.Country));
                await command.ExecuteNonQueryAsync();

                // Get the OrderID of the newly inserted order
                command.CommandText = "SELECT last_insert_rowid()";
                var result = await command.ExecuteScalarAsync() ?? throw new RepositoryException("Failed to retrieve the order ID.");
                var orderId = (long)result;

                // Add OrderDetails
                foreach (var orderDetail in order.OrderDetails)
                {
                    // Add OrderDetail
                    command.CommandText = "INSERT INTO OrderDetails (OrderID, ProductID, UnitPrice, Quantity, Discount) VALUES (@OrderID, @ProductID, @UnitPrice, @Quantity, @Discount)";
                    command.Parameters.Clear();
                    command.Parameters.Add(CreateParameter(command, "@OrderID", orderId));
                    command.Parameters.Add(CreateParameter(command, "@ProductID", orderDetail.Product.Id));
                    command.Parameters.Add(CreateParameter(command, "@UnitPrice", orderDetail.UnitPrice));
                    command.Parameters.Add(CreateParameter(command, "@Quantity", orderDetail.Quantity));
                    command.Parameters.Add(CreateParameter(command, "@Discount", orderDetail.Discount));
                    await command.ExecuteNonQueryAsync();
                }

                await sqlTran.CommitAsync();
                await this.context.CloseAsync();
                return orderId;
            }
            catch (Exception ex)
            {
                await sqlTran.RollbackAsync();
                await this.context.CloseAsync();
                throw new RepositoryException(ex.Message, ex);
            }
        }

        private async Task<IList<Order>> GetOrdersInternalAsync(int skip, int count)
        {
            var orders = new List<Order>();

            await this.context.OpenAsync();
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT * FROM Orders ORDER BY OrderID LIMIT @Count OFFSET @Skip";
            command.Parameters.Add(CreateParameter(command, "@Count", count));
            command.Parameters.Add(CreateParameter(command, "@Skip", skip));

            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var orderId = reader.GetInt64(reader.GetOrdinal("OrderID"));
                    var order = new Order(orderId)
                    {
                        OrderDate = reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                        RequiredDate = reader.GetDateTime(reader.GetOrdinal("RequiredDate")),
                        ShippedDate = reader.GetDateTime(reader.GetOrdinal("ShippedDate")),
                        Freight = reader.GetDouble(reader.GetOrdinal("Freight")),
                        ShipName = reader.GetString(reader.GetOrdinal("ShipName")),
                        ShippingAddress = new ShippingAddress(
                            reader.GetString(reader.GetOrdinal("ShipAddress")),
                            reader.GetString(reader.GetOrdinal("ShipCity")),
                            reader.IsDBNull(reader.GetOrdinal("ShipRegion")) ? null : reader.GetString(reader.GetOrdinal("ShipRegion")),
                            reader.GetString(reader.GetOrdinal("ShipPostalCode")),
                            reader.GetString(reader.GetOrdinal("ShipCountry"))),
                        Customer = new Customer(new CustomerCode(reader.GetString(reader.GetOrdinal("CustomerID"))))
                        {
                            CompanyName = await this.GetCustomerCompanyNameAsync(reader.GetString(reader.GetOrdinal("CustomerID"))) ?? "Default Company Name",
                        },
                        Employee = await this.GetEmployeeAsync(reader.GetInt64(reader.GetOrdinal("EmployeeID"))),
                        Shipper = await this.GetShipperAsync(reader.GetInt64(reader.GetOrdinal("ShipVia"))),
                    };

                    await this.GetOrderDetailsAsync(order);
                    orders.Add(order);
                }
            }

            await this.context.CloseAsync();
            return orders;
        }

        private async Task<string?> GetCustomerCompanyNameAsync(string customerId)
        {
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT CompanyName FROM Customers WHERE CustomerID = @CustomerID";
            command.Parameters.Add(CreateParameter(command, "@CustomerID", customerId));
            return (string?)await command.ExecuteScalarAsync();
        }

        private async Task<Employee> GetEmployeeAsync(long employeeId)
        {
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT * FROM Employees WHERE EmployeeID = @EmployeeID";
            command.Parameters.Add(CreateParameter(command, "@EmployeeID", employeeId));

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return new Employee(employeeId)
                {
                    FirstName = reader.GetString(reader.GetOrdinal("FirstName")),
                    LastName = reader.GetString(reader.GetOrdinal("LastName")),
                    Country = reader.GetString(reader.GetOrdinal("Country")),
                };
            }

            throw new RepositoryException($"Employee with ID {employeeId} not found.");
        }

        private async Task<Shipper> GetShipperAsync(long shipperId)
        {
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT * FROM Shippers WHERE ShipperID = @ShipperID";
            command.Parameters.Add(CreateParameter(command, "@ShipperID", shipperId));

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return new Shipper(shipperId)
                {
                    CompanyName = reader.GetString(reader.GetOrdinal("CompanyName")),
                };
            }

            throw new RepositoryException($"Shipper with ID {shipperId} not found.");
        }

        private async Task GetOrderDetailsAsync(Order order)
        {
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT * FROM OrderDetails WHERE OrderID = @OrderID";
            command.Parameters.Add(CreateParameter(command, "@OrderID", order.Id));

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var productId = reader.GetInt64(reader.GetOrdinal("ProductID"));
                var product = await this.GetProductAsync(productId);
                order.OrderDetails.Add(new OrderDetail(order)
                {
                    Product = product,
                    UnitPrice = reader.GetDouble(reader.GetOrdinal("UnitPrice")),
                    Quantity = reader.GetInt32(reader.GetOrdinal("Quantity")),
                    Discount = reader.GetDouble(reader.GetOrdinal("Discount")),
                });
            }
        }

        private async Task<Product> GetProductAsync(long productId)
        {
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT * FROM Products WHERE ProductID = @ProductID";
            command.Parameters.Add(CreateParameter(command, "@ProductID", productId));

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return new Product(productId)
                {
                    ProductName = reader.GetString(reader.GetOrdinal("ProductName")),
                    SupplierId = reader.GetInt64(reader.GetOrdinal("SupplierID")),
                    CategoryId = reader.GetInt64(reader.GetOrdinal("CategoryID")),
                    Supplier = await this.GetSupplierNameAsync(reader.GetInt64(reader.GetOrdinal("SupplierID"))) ?? "Default Supplier Name",
                    Category = await this.GetCategoryNameAsync(reader.GetInt64(reader.GetOrdinal("CategoryID"))) ?? "Default Category Name",
                };
            }

            throw new RepositoryException($"Product with ID {productId} not found.");
        }

        private async Task<string?> GetSupplierNameAsync(long supplierId)
        {
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT CompanyName FROM Suppliers WHERE SupplierID = @SupplierID";
            command.Parameters.Add(CreateParameter(command, "@SupplierID", supplierId));
            return (string?)await command.ExecuteScalarAsync();
        }

        private async Task<string?> GetCategoryNameAsync(long categoryId)
        {
            var command = this.context.CreateCommand();
            command.CommandText = "SELECT CategoryName FROM Categories WHERE CategoryID = @CategoryID";
            command.Parameters.Add(CreateParameter(command, "@CategoryID", categoryId));
            return (string?)await command.ExecuteScalarAsync();
        }
    }
}
