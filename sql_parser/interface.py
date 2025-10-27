from .parser import parse
from .executor import Executor

def execute_sql(db_manager, sql: str):
    try:
        plans = parse(sql)
        if not plans:
            return "ERROR: No se pudieron parsear consultas"

        executor = Executor(db_manager)

        results = []
        for plan in plans:
            try:
                result = executor.execute(plan)
                results.append(result)
            except Exception as e:
                results.append(f"ERROR: {e}")

        return results[0] if len(results) == 1 else results

    except Exception as e:
        return f"ERROR: {e}"