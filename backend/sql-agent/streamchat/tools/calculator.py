import sympy


def calculate(expression: str) -> str:
    """Calculate a mathematical expression.
    Args:
        expression (str): Python arithmetic expression to calculate.
    Returns:
        str: The result of the calculation.
    """
    try:
        result = sympy.sympify(expression).evalf()
        return str(result)
    except Exception as e:
        return f"Error calculating expression: {e}"
