using RideHailing.BuildingBlocks.Data;
using RideHailing.BuildingBlocks.Data.Interfaces;
using RideHailing.BuildingBlocks.Outbox;
using Rides.Api.Rides;
using Rides.Api.Rides.Endpoints;
using Rides.Api.Rides.Repositories;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddValidation();
builder.Services.AddProblemDetails();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var connectionString = builder.Configuration.GetConnectionString("RideServiceDb")
    ?? throw new InvalidOperationException("Connection string 'RideServiceDb' is missing.");

// services
builder.Services.AddSingleton<IDbConnectionFactory>(new SqlConnectionFactory(connectionString));

builder.Services.AddScoped<IUnitOfWork, UnitOfWork>();
builder.Services.AddScoped<RideRepository>();
builder.Services.AddScoped<OutboxRepository>();
builder.Services.AddScoped<RequestRideHandler>();

builder.Services.AddSingleton(TimeProvider.System);

var app = builder.Build();

// unhandled ex to ProblemDetails 500
app.UseExceptionHandler();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapRideEndpoints();

app.Run();
