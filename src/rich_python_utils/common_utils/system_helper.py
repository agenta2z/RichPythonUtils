import platform
from enum import StrEnum


class OperatingSystem(StrEnum):
    """String-based enum representing common operating systems."""
    WINDOWS = "windows"
    LINUX = "linux"
    MACOS = "macos"
    ANDROID = "android"
    IOS = "ios"
    UNKNOWN = "unknown"


def get_current_platform(
        identify_mobile_operating_system: bool = True,
        system_str: str = None,
        platform_str: str = None
) -> OperatingSystem:
    """Detects the current operating system, optionally overriding system/platform strings.

    This function inspects either:
      1. The provided `system_str` and `platform_str`, if supplied, or
      2. The result of `platform.system()` and `platform.platform()` if they are not.

    It returns one of the enum values from OperatingSystem:
        - WINDOWS
        - LINUX
        - MACOS
        - ANDROID (if `identify_mobile_operating_system=True` and heuristics detect it)
        - IOS (if `identify_mobile_operating_system=True` and heuristics detect it)
        - UNKNOWN

    Args:
        identify_mobile_operating_system (bool, optional):
            If True (default), attempts additional heuristics to distinguish Android or iOS
            from generic 'Linux' or 'Darwin' outputs. If False, Android-based environments
            will be reported as LINUX, and iOS-based environments as MACOS.
        system_str (str, optional):
            A custom string to mimic the output of `platform.system().lower()`.
            If None, the function calls `platform.system()` internally.
        platform_str (str, optional):
            A custom string to mimic the output of `platform.platform().lower()`.
            If None, the function calls `platform.platform()` internally.

    Returns:
        OperatingSystem: An enum value representing the detected operating system.

    Examples:
        >>> ios_os = get_current_platform(
        ...     system_str='darwin',
        ...     platform_str='Darwin-iPhoneOS-16.0.0'
        ... )
        >>> print(ios_os)
        ios

        >>> macos = get_current_platform(
        ...     system_str='darwin',
        ...     platform_str='Darwin-iPhoneOS-16.0.0',
        ...     identify_mobile_operating_system=False
        ... )
        >>> print(macos)
        macos

        >>> android_os = get_current_platform(
        ...     system_str='linux',
        ...     platform_str='Linux-5.4.0-android'
        ... )
        >>> print(android_os)
        android

    Notes:
        - The detection for Android or iOS relies on heuristics:
          * 'platform.system()' often returns 'Linux' on Android, 'Darwin' on iOS.
          * 'platform.platform()' might include keywords like 'android', 'iphone', 'ios', etc.
          These checks are not guaranteed for all environments or Python distributions.
    """
    system_str = (system_str or platform.system()).lower()  # e.g., "windows", "linux", "darwin"
    platform_str = (platform_str or platform.platform()).lower()  # e.g., "linux-5.4.0-android...", "darwin-20.6.0..."

    if "darwin" in system_str:
        # Normally macOS, unless 'identify_mobile_operating_system' is True
        # and we see an iOS hint in the platform string
        if identify_mobile_operating_system:
            # Check for iOS-related keywords
            if any(x in platform_str for x in ["iphone", "ios", "iphonesimulator"]):
                return OperatingSystem.IOS
        return OperatingSystem.MACOS

    if "win" in system_str:
        return OperatingSystem.WINDOWS

    if "linux" in system_str:
        # Typically Linux, but attempt Android detection if enabled
        if identify_mobile_operating_system and "android" in platform_str:
            return OperatingSystem.ANDROID
        return OperatingSystem.LINUX

    # If none of the above conditions are met, return UNKNOWN
    return OperatingSystem.UNKNOWN
