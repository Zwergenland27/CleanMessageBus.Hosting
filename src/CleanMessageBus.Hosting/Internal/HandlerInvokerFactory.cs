using System.Linq.Expressions;
using System.Reflection;
using CleanMessageBus.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace CleanMessageBus.Hosting.Internal;

/// <summary>
/// Inspects a handler delegate once at registration and compiles an invoker, so no reflection is needed per message.
/// </summary>
/// <remarks>
/// Parameter binding:
/// the first parameter is the event; <see cref="EventContext"/>, <see cref="MessageEnvelope"/>,
/// <see cref="CancellationToken"/> and <see cref="IServiceProvider"/> are provided by the bus;
/// parameters with <see cref="FromKeyedServicesAttribute"/> are resolved as keyed services;
/// all other parameters are resolved from the handler scope (optional if they have a default value).
/// </remarks>
internal static class HandlerInvokerFactory
{
    private static readonly MethodInfo GetRequiredServiceMethod =
        typeof(ServiceProviderServiceExtensions).GetMethod(
            nameof(ServiceProviderServiceExtensions.GetRequiredService),
            [typeof(IServiceProvider), typeof(Type)])!;

    private static readonly MethodInfo GetRequiredKeyedServiceMethod =
        typeof(ServiceProviderKeyedServiceExtensions).GetMethod(
            nameof(ServiceProviderKeyedServiceExtensions.GetRequiredKeyedService),
            [typeof(IServiceProvider), typeof(Type), typeof(object)])!;

    private static readonly MethodInfo ResolveOptionalMethod =
        typeof(HandlerInvokerFactory).GetMethod(nameof(ResolveOptional), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo ResolveOptionalKeyedMethod =
        typeof(HandlerInvokerFactory).GetMethod(nameof(ResolveOptionalKeyed), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly ConstructorInfo ValueTaskFromTaskConstructor =
        typeof(ValueTask).GetConstructor([typeof(Task)])!;

    private static readonly PropertyInfo EnvelopeProperty =
        typeof(EventContext).GetProperty(nameof(EventContext.Envelope))!;

    /// <param name="eventType">Type of the handled event; must be the type of the first parameter.</param>
    /// <param name="handler">The handler delegate.</param>
    /// <param name="validationServices">
    /// Root service provider used to verify at registration that all parameters can be resolved. Optional.
    /// </param>
    public static HandlerInvoker Create(Type eventType, Delegate handler, IServiceProvider? validationServices)
    {
        ArgumentNullException.ThrowIfNull(eventType);
        ArgumentNullException.ThrowIfNull(handler);

        var method = handler.Method;
        var parameters = method.GetParameters();
        if (parameters.Length == 0 || parameters[0].ParameterType != eventType)
        {
            throw new ArgumentException(
                $"The first parameter of the event handler must be of type '{eventType.FullName}'.",
                nameof(handler));
        }

        var isService = validationServices?.GetService<IServiceProviderIsService>();
        var isKeyedService = validationServices?.GetService<IServiceProviderIsKeyedService>();

        var servicesParameter = Expression.Parameter(typeof(IServiceProvider), "services");
        var contextParameter = Expression.Parameter(typeof(EventContext), "context");
        var eventParameter = Expression.Parameter(typeof(object), "event");
        var cancellationParameter = Expression.Parameter(typeof(CancellationToken), "cancellationToken");

        var arguments = new Expression[parameters.Length];
        arguments[0] = Expression.Convert(eventParameter, eventType);
        for (var i = 1; i < parameters.Length; i++)
        {
            arguments[i] = BindParameter(
                parameters[i],
                servicesParameter,
                contextParameter,
                cancellationParameter,
                isService,
                isKeyedService);
        }

        var call = Expression.Invoke(Expression.Constant(handler), arguments);
        var body = ConvertToValueTask(call, method.ReturnType);

        return Expression
            .Lambda<HandlerInvoker>(body, servicesParameter, contextParameter, eventParameter, cancellationParameter)
            .Compile();
    }

    private static Expression BindParameter(
        ParameterInfo parameter,
        ParameterExpression services,
        ParameterExpression context,
        ParameterExpression cancellationToken,
        IServiceProviderIsService? isService,
        IServiceProviderIsKeyedService? isKeyedService)
    {
        var type = parameter.ParameterType;
        if (type.IsByRef)
        {
            throw new ArgumentException($"Parameter '{parameter.Name}' of the event handler must not be passed by reference.");
        }

        if (type == typeof(CancellationToken))
        {
            return cancellationToken;
        }

        if (type == typeof(EventContext))
        {
            return context;
        }

        if (type == typeof(MessageEnvelope))
        {
            return Expression.Property(context, EnvelopeProperty);
        }

        if (type == typeof(IServiceProvider))
        {
            return services;
        }

        var typeConstant = Expression.Constant(type, typeof(Type));
        var keyed = parameter.GetCustomAttribute<FromKeyedServicesAttribute>();
        if (keyed is not null)
        {
            var keyConstant = Expression.Constant(keyed.Key, typeof(object));
            if (parameter.HasDefaultValue)
            {
                return Expression.Convert(
                    Expression.Call(
                        ResolveOptionalKeyedMethod,
                        services,
                        typeConstant,
                        keyConstant,
                        Expression.Constant(parameter.DefaultValue, typeof(object))),
                    type);
            }

            if (isKeyedService is not null && !isKeyedService.IsKeyedService(type, keyed.Key))
            {
                throw UnresolvableParameter(parameter, $" with key '{keyed.Key}'");
            }

            return Expression.Convert(
                Expression.Call(GetRequiredKeyedServiceMethod, services, typeConstant, keyConstant),
                type);
        }

        if (parameter.HasDefaultValue)
        {
            return Expression.Convert(
                Expression.Call(
                    ResolveOptionalMethod,
                    services,
                    typeConstant,
                    Expression.Constant(parameter.DefaultValue, typeof(object))),
                type);
        }

        if (isService is not null && !isService.IsService(type))
        {
            throw UnresolvableParameter(parameter, string.Empty);
        }

        return Expression.Convert(Expression.Call(GetRequiredServiceMethod, services, typeConstant), type);
    }

    private static Expression ConvertToValueTask(Expression call, Type returnType)
    {
        if (returnType == typeof(void))
        {
            return Expression.Block(call, Expression.Default(typeof(ValueTask)));
        }

        if (returnType == typeof(ValueTask))
        {
            return call;
        }

        if (typeof(Task).IsAssignableFrom(returnType))
        {
            return Expression.New(ValueTaskFromTaskConstructor, Expression.Convert(call, typeof(Task)));
        }

        if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            var asTask = Expression.Call(call, returnType.GetMethod(nameof(ValueTask<object>.AsTask))!);
            return Expression.New(ValueTaskFromTaskConstructor, Expression.Convert(asTask, typeof(Task)));
        }

        throw new ArgumentException(
            $"Event handlers must return void, Task, ValueTask, Task<T> or ValueTask<T>, but return '{returnType.FullName}'.");
    }

    private static InvalidOperationException UnresolvableParameter(ParameterInfo parameter, string keyInfo) =>
        new($"Unable to resolve service '{parameter.ParameterType.FullName}'{keyInfo} for parameter "
            + $"'{parameter.Name}' of the event handler. Register the service or give the parameter a default value.");

    private static object? ResolveOptional(IServiceProvider services, Type type, object? defaultValue) =>
        services.GetService(type) ?? defaultValue;

    private static object? ResolveOptionalKeyed(IServiceProvider services, Type type, object? key, object? defaultValue) =>
        services is IKeyedServiceProvider keyed ? keyed.GetKeyedService(type, key) ?? defaultValue : defaultValue;
}