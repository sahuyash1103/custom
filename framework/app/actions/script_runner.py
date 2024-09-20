import logging
import io
import contextlib

def run_script(script: str):
    try:
        # Capture the output of the script
        logging.info("Running script")
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            exec(script)

        # Return the output
        result = output.getvalue()
        logging.info(f"Script completed successfully with output: {result}")
        return result

    except Exception as e:
        logging.error(f"Failed to run script: {e}")
        return f"Failed to run script: {e}"