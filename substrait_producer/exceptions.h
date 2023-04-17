#pragma once

#include <exception>
#include <string>
using namespace std;

class DataTypeNotMatchException : public std::exception
{
    virtual const char *what() const throw()
    {
        return "Data type does not match";
    }
};

class DoubleWithDifferentPrecisionException : public std::exception
{
    virtual const char *what() const throw()
    {
        return "Double data type should have same precision";
    }
};

class DataTypeException : public std::exception
{
    virtual const char *what() const throw()
    {
        return "Data type error";
    }
};

class UnimplementedFunctionException : public std::exception
{
    virtual const char *what() const throw()
    {
        return "The function has not been implemented yet";
    }
};

class IntervalException : public std::exception
{
    virtual const char *what() const throw()
    {
        return "Interval error";
    }
};

class BoundaryException : public std::exception
{
    virtual const char *what() const throw()
    {
        return "Boundary error";
    }
};

class ExpressionException : public std::exception
{
    virtual const char *what() const throw()
    {
        return "Exception happens in expressions";
    }
};

class Exception : public std::exception
{
public:
    Exception(const std::string &message) : message(message)
    {
    }

    virtual const char *what() const throw()
    {
        return message.c_str();
    }

private:
    string message;
};