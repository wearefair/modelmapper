import sys
import os
import termios
import fcntl


def get_user_input(message, validate_func, **kwargs):
    user_input = None
    sys.stdout.write(message)
    while user_input is None or not validate_func(user_input=user_input, **kwargs):
        if user_input is not None:
            sys.stdout.write(f"{user_input} is not valid for {', '.join(kwargs.values())}")
        user_input = input()
    sys.stdout.write(f"{user_input} works!")
    return user_input


YES_NO_CHOICES = {
    'y': {'help': 'to continue', 'func': lambda x: True},
    'n': {'help': 'to abort', 'func': lambda x: sys.exit}
}


def get_user_choice(message, choices, **kwargs):
    pressed = None
    msgs = [message]
    allowed_keys = set()
    for key, value in choices.items():
        msg = f"Press ({key}) {value['help']}."
        msgs.append(msg)
        allowed_keys.add(key)
    sys.stdout.write(' '.join(msgs))

    while pressed not in allowed_keys:
        if pressed is not None:
            sys.stdout.write(f'{pressed} is not a valid option.')
        pressed = read_single_keypress()

    sys.stdout.write(pressed)

    return choices[pressed]['func'](pressed, **kwargs)


def read_single_keypress():
    """Waits for a single keypress on stdin.
    https://stackoverflow.com/a/6599441/1497443

    This is a silly function to call if you need to do it a lot because it has
    to store stdin's current setup, setup stdin for reading single keystrokes
    then read the single keystroke then revert stdin back after reading the
    keystroke.

    Returns the character of the key that was pressed (zero on
    KeyboardInterrupt which can happen when a signal gets handled)

    """
    fd = sys.stdin.fileno()
    # save old state
    flags_save = fcntl.fcntl(fd, fcntl.F_GETFL)
    attrs_save = termios.tcgetattr(fd)
    # make raw - the way to do this comes from the termios(3) man page.
    attrs = list(attrs_save) # copy the stored version to update
    # iflag
    attrs[0] &= ~(termios.IGNBRK | termios.BRKINT | termios.PARMRK
                  | termios.ISTRIP | termios.INLCR | termios. IGNCR
                  | termios.ICRNL | termios.IXON )
    # oflag
    attrs[1] &= ~termios.OPOST
    # cflag
    attrs[2] &= ~(termios.CSIZE | termios. PARENB)
    attrs[2] |= termios.CS8
    # lflag
    attrs[3] &= ~(termios.ECHONL | termios.ECHO | termios.ICANON
                  | termios.ISIG | termios.IEXTEN)
    termios.tcsetattr(fd, termios.TCSANOW, attrs)
    # turn off non-blocking
    fcntl.fcntl(fd, fcntl.F_SETFL, flags_save & ~os.O_NONBLOCK)
    # read a single keystroke
    try:
        ret = sys.stdin.read(1) # returns a single character
    except KeyboardInterrupt:
        ret = 0
    finally:
        # restore old state
        termios.tcsetattr(fd, termios.TCSAFLUSH, attrs_save)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags_save)
    return ret
