from camoufox.sync_api import Camoufox
import requests
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import threading
import asyncio
import urllib3
import ssl
import certifi
import time
import argparse


def detect_onetrust_config(page):
    """
    Detect OneTrust configuration from the page to create accurate cookies.
    Returns a dictionary with OneTrust configuration details.
    """
    try:
        config = page.evaluate("""
            () => {
                // Try to extract OneTrust configuration
                const config = {
                    version: null,
                    hosts: [],
                    groups: [],
                    geolocation: null,
                    scriptId: null
                };

                // Look for OneTrust script elements
                const scripts = document.querySelectorAll('script[src*="onetrust"], script[src*="optanon"]');
                for (const script of scripts) {
                    const src = script.src;
                    // Extract script ID from URL
                    const match = src.match(/optanon\\.(\\w+)\\.js/);
                    if (match) {
                        config.scriptId = match[1];
                    }
                }

                // Look for existing OneTrust cookies for version info
                const existingCookies = document.cookie;
                const versionMatch = existingCookies.match(/version=([^&;]+)/);
                if (versionMatch) {
                    config.version = versionMatch[1];
                }

                // Try to get OneTrust global object if available
                if (window.OneTrust && window.OneTrust.GetDomainData) {
                    try {
                        const domainData = window.OneTrust.GetDomainData();
                        if (domainData) {
                            config.version = domainData.ScriptVersion || domainData.version;
                            config.geolocation = domainData.GeolocationRuleGroupId;
                        }
                    } catch (e) {
                        // OneTrust not fully loaded yet
                    }
                }

                // Look for OneTrust configuration in window
                if (window.OptanonActiveGroups) {
                    config.groups = window.OptanonActiveGroups.split(',');
                }

                // Extract hosts from any existing OptanonConsent cookie
                const consentMatch = existingCookies.match(/OptanonConsent=([^;]+)/);
                if (consentMatch) {
                    const consentValue = decodeURIComponent(consentMatch[1]);
                    const hostsMatch = consentValue.match(/hosts=([^&]*)/);
                    if (hostsMatch && hostsMatch[1]) {
                        config.hosts = hostsMatch[1].split(',').filter(h => h.trim());
                    }

                    // Extract groups from existing cookie
                    const groupsMatch = consentValue.match(/groups=([^&]*)/);
                    if (groupsMatch && groupsMatch[1]) {
                        config.groups = groupsMatch[1].split(',').map(g => g.split(':')[0]);
                    }
                }

                return config;
            }
        """)

        # Set defaults if nothing detected
        if not config.get('version'):
            config['version'] = '6.33.0'  # fallback version

        if not config.get('groups'):
            config['groups'] = ['C0001', 'C0002', 'C0003', 'C0004']  # standard groups

        print(f"  OneTrust config detected: version={config['version']}, groups={len(config['groups'])}")
        return config

    except Exception as e:
        print(f"  Warning: Could not detect OneTrust config: {e}")
        return {
            'version': '6.33.0',
            'groups': ['C0001', 'C0002', 'C0003', 'C0004'],
            'hosts': [],
            'geolocation': None,
            'scriptId': None
        }


def inject_onetrust_javascript(page):
    """
    Inject JavaScript to programmatically set OneTrust consent using native APIs.
    This approach uses OneTrust's own methods to set consent state.
    """
    try:
        result = page.evaluate("""
            () => {
                return new Promise((resolve) => {
                    // Function to set consent via OneTrust API
                    const setOneTrustConsent = () => {
                        try {
                            // Check if OneTrust is available
                            if (typeof window.OneTrust !== 'undefined') {
                                // Accept all consent categories
                                if (window.OneTrust.AllowAll) {
                                    window.OneTrust.AllowAll();
                                    return 'OneTrust.AllowAll() called';
                                }

                                // Alternative: Set individual groups
                                if (window.OneTrust.UpdateConsent) {
                                    const groups = ['C0001', 'C0002', 'C0003', 'C0004'];
                                    groups.forEach(group => {
                                        window.OneTrust.UpdateConsent('Group', group + ':1');
                                    });
                                    return 'OneTrust.UpdateConsent() called for groups';
                                }
                            }

                            // Check for OneTrust cookie banner and simulate accept
                            if (typeof window.Optanon !== 'undefined' && window.Optanon.TriggerGoogleAnalyticsEvent) {
                                window.Optanon.TriggerGoogleAnalyticsEvent('OneTrust', 'All Cookies Accepted', 'Optanon');
                                return 'Optanon accept event triggered';
                            }

                            // Try to trigger accept all button click programmatically
                            const acceptBtn = document.querySelector('#onetrust-accept-btn-handler, .onetrust-close-btn-handler');
                            if (acceptBtn && acceptBtn.offsetParent !== null) {
                                acceptBtn.click();
                                return 'Accept button clicked programmatically';
                            }

                            return 'No OneTrust API available';

                        } catch (error) {
                            return 'Error: ' + error.message;
                        }
                    };

                    // Try immediately
                    let result = setOneTrustConsent();

                    // If OneTrust not ready, wait and try again
                    if (result === 'No OneTrust API available') {
                        setTimeout(() => {
                            result = setOneTrustConsent();
                            resolve(result);
                        }, 2000);
                    } else {
                        resolve(result);
                    }
                });
            }
        """)

        print(f"  JavaScript injection result: {result}")
        return result != 'No OneTrust API available'

    except Exception as e:
        print(f"  Error in JavaScript injection: {e}")
        return False


def set_onetrust_cookies(page, config=None):
    """
    Set OneTrust consent cookies to bypass privacy modals.
    This prevents the OneTrust banner from appearing by setting appropriate consent cookies.
    Uses dynamic configuration if provided.
    """
    try:
        # Use provided config or detect dynamically
        if not config:
            config = detect_onetrust_config(page)

        # Get current timestamp for cookie values
        current_time = datetime.now(timezone.utc)
        timestamp_iso = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Build groups string with consent (1 = accept, 0 = reject)
        groups_list = []
        for group in config['groups']:
            groups_list.append(f"{group}:1")

        # Add IAB TCF groups if not present
        iab_groups = ['IAB2V2_1', 'IAB2V2_2', 'IAB2V2_3', 'IAB2V2_4', 'IAB2V2_5',
                      'IAB2V2_6', 'IAB2V2_7', 'IAB2V2_8', 'IAB2V2_9', 'IAB2V2_10', 'IAB2V2_11']
        for iab_group in iab_groups:
            if iab_group not in config['groups']:
                groups_list.append(f"{iab_group}:1")

        groups_string = ','.join(groups_list)

        # Build hosts string
        hosts_string = ','.join(config.get('hosts', []))

        # Create comprehensive consent string
        optanon_consent = (
            f"groups={groups_string}&"
            f"datestamp={timestamp_iso}&"
            f"version={config['version']}&"
            f"hosts={hosts_string}&"
            f"landingPath=NotLandingPage&"
            f"AwaitingReconsent=false&"
            f"geolocation={config.get('geolocation', '')}&"
            f"isAnonUser=1&"
            f"consentId={config.get('scriptId', '')}&"
            f"interactionCount=1&"
            f"isIABGlobal=true"
        )

        # Cookie to indicate banner was closed
        optanon_alert_closed = timestamp_iso

        # Multiple domain variations for TripAdvisor
        domains = ['.tripadvisor.com', '.tripadvisor.co.uk', '.tripadvisor.ca',
                  '.tripadvisor.com.au', '.tripadvisor.fr', '.tripadvisor.de']

        cookies_to_set = []

        # Add cookies for each domain
        for domain in domains:
            cookies_to_set.extend([
                {
                    'name': 'OptanonConsent',
                    'value': optanon_consent,
                    'domain': domain,
                    'path': '/',
                    'secure': True,
                    'sameSite': 'Lax'
                },
                {
                    'name': 'OptanonAlertBoxClosed',
                    'value': optanon_alert_closed,
                    'domain': domain,
                    'path': '/',
                    'secure': True,
                    'sameSite': 'Lax'
                },
                {
                    'name': 'eupubconsent-v2',
                    'value': 'CPuqK4APuqK4AAcABBENB2CsAP_AAH_AAAAAKpdf_X__b2_j-_5_f_t0eY1P9_7__-0zjhfdt-8N2f_X_L8X42M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrzPsbk2cr7NKJ7PEmnMbO2dYGH9_n93TuZKY7_7__gAAAAAAAAAAA',
                    'domain': domain,
                    'path': '/',
                    'secure': True,
                    'sameSite': 'None'
                }
            ])

        # Set the cookies
        success_count = 0
        for cookie in cookies_to_set:
            try:
                page.context.add_cookies([cookie])
                success_count += 1
            except Exception as e:
                print(f"  Warning: Could not set {cookie['name']} cookie for {cookie['domain']}: {e}")

        print(f"  ✓ OneTrust consent cookies set ({success_count}/{len(cookies_to_set)}) with dynamic config")
        print(f"    Version: {config['version']}, Groups: {len(groups_list)}")

        # Also set localStorage consent state
        try:
            page.evaluate(f"""
                () => {{
                    // Set localStorage consent indicators
                    localStorage.setItem('OneTrustWildcardDomainData', '{optanon_consent}');
                    localStorage.setItem('OneTrustActiveGroups', '{groups_string}');
                    localStorage.setItem('OneTrustConsent', '1');

                    // Set sessionStorage as backup
                    sessionStorage.setItem('OneTrustConsent', '1');
                    sessionStorage.setItem('OptanonActiveGroups', '{groups_string}');
                }}
            """)
            print("  ✓ localStorage/sessionStorage consent state set")
        except Exception as e:
            print(f"  Warning: Could not set localStorage: {e}")

        return success_count > 0

    except Exception as e:
        print(f"  Error setting OneTrust cookies: {e}")
        return False


def handle_onetrust_modal_enhanced(page):
    """
    Enhanced OneTrust modal handler with multiple strategies.
    Tries JavaScript API first, then fallback to clicking strategies.
    """
    try:
        print("  Enhanced OneTrust modal handling...")

        # Strategy 1: Try JavaScript API injection first
        try:
            js_success = inject_onetrust_javascript(page)
            if js_success:
                print("  ✓ OneTrust handled via JavaScript API")
                return True
        except Exception as e:
            print(f"  JavaScript API approach failed: {e}")

        # Strategy 2: Comprehensive modal detection and clicking based on actual HTML structure
        onetrust_selectors = [
            # Based on the provided HTML structure - exact selectors
            'button.ot-pc-refuse-all-handler',  # "Reject All" in preference center
            'button.save-preference-btn-handler.onetrust-close-btn-handler',  # "Confirm My Choices"
            '#accept-recommended-btn-handler',  # "Allow All" (hidden by default)

            # Primary banner buttons
            '#onetrust-accept-btn-handler',     # Main accept button
            '#onetrust-reject-all-handler',     # Main reject button
            '#onetrust-pc-btn-handler',         # Show purposes button

            # Floating button variations
            'button.ot-floating-button__close',
            'button[aria-label="Close Preferences"]',

            # Generic text-based selectors as fallback
            'button:has-text("Reject All")',
            'button:has-text("Allow All")',
            'button:has-text("Confirm My Choices")',
            'button:has-text("Accept All")'
        ]

        # Try each selector with enhanced approach
        for selector in onetrust_selectors:
            try:
                # Check if button exists and is visible
                button = page.locator(selector).first
                if button.count() > 0:
                    # Make sure element is visible
                    page.evaluate(f"""
                        () => {{
                            const btn = document.querySelector('{selector}');
                            if (btn) {{
                                btn.style.display = 'block';
                                btn.style.visibility = 'visible';
                                btn.style.opacity = '1';
                                btn.style.pointerEvents = 'auto';
                                // Remove hidden attributes
                                btn.removeAttribute('aria-hidden');
                                btn.removeAttribute('hidden');
                            }}
                        }}
                    """)

                    # Try to click
                    button.scroll_into_view_if_needed(timeout=2000)
                    page.wait_for_timeout(500)
                    button.click(force=True, timeout=3000)

                    print(f"  ✓ OneTrust modal handled with selector: {selector}")
                    page.wait_for_timeout(2000)

                    # Verify modal is gone
                    modal_gone = page.evaluate("""
                        () => {
                            const modal = document.querySelector('#onetrust-consent-sdk, #onetrust-pc-sdk');
                            return !modal || modal.style.display === 'none' || !modal.offsetParent;
                        }
                    """)

                    if modal_gone:
                        return True

            except Exception as e:
                print(f"  Selector {selector} failed: {e}")
                continue

        # Strategy 3: Handle the specific modal structure from the provided HTML
        try:
            # The provided HTML shows a preference center modal - handle this specifically
            preference_center_result = page.evaluate("""
                () => {
                    // First, try to show the hidden "Allow All" button
                    const allowAllBtn = document.getElementById('accept-recommended-btn-handler');
                    if (allowAllBtn) {
                        // Force show the button
                        allowAllBtn.style.display = 'inline-block';
                        allowAllBtn.style.visibility = 'visible';
                        allowAllBtn.removeAttribute('aria-hidden');
                        allowAllBtn.tabIndex = 0;

                        // Click it
                        allowAllBtn.click();
                        return 'allow_all_unhidden_and_clicked';
                    }

                    // If that fails, look for the footer buttons in preference center
                    const rejectAllBtn = document.querySelector('.ot-pc-refuse-all-handler');
                    if (rejectAllBtn) {
                        rejectAllBtn.click();
                        return 'reject_all_clicked';
                    }

                    const confirmBtn = document.querySelector('.save-preference-btn-handler.onetrust-close-btn-handler');
                    if (confirmBtn) {
                        confirmBtn.click();
                        return 'confirm_choices_clicked';
                    }

                    return 'no_buttons_found';
                }
            """)

            print(f"  Preference center handling: {preference_center_result}")

            if preference_center_result in ['allow_all_unhidden_and_clicked', 'reject_all_clicked', 'confirm_choices_clicked']:
                page.wait_for_timeout(3000)  # Wait for modal to close

                # Verify modal is gone
                modal_gone = page.evaluate("""
                    () => {
                        const modal = document.querySelector('#onetrust-consent-sdk');
                        return !modal || modal.style.display === 'none' || !modal.offsetParent;
                    }
                """)

                if modal_gone:
                    print("  ✓ OneTrust modal closed via preference center")
                    return True
        except Exception as e:
            print(f"  Preference center handling failed: {e}")

        # Strategy 4: Aggressive modal removal as last resort
        try:
            result = page.evaluate("""
                () => {
                    let removed = 0;

                    // Remove all OneTrust elements completely
                    const elements = ['#onetrust-consent-sdk', '#onetrust-pc-sdk', '#onetrust-banner-sdk',
                                     '.onetrust-pc-dark-filter', '.ot-fade-in', '.ot-sdk-not-webkit'];

                    elements.forEach(selector => {
                        const els = document.querySelectorAll(selector);
                        els.forEach(el => {
                            if (el) {
                                el.parentNode.removeChild(el);
                                removed++;
                            }
                        });
                    });

                    // Also remove by checking for OneTrust classes
                    const allElements = document.querySelectorAll('*');
                    allElements.forEach(el => {
                        if (el.id && (el.id.includes('onetrust') || el.id.includes('optanon'))) {
                            try {
                                el.parentNode.removeChild(el);
                                removed++;
                            } catch (e) {}
                        }
                    });

                    // Disable any OneTrust scripts
                    const scripts = document.querySelectorAll('script[src*="onetrust"], script[src*="optanon"]');
                    scripts.forEach(script => {
                        script.src = '';
                        script.innerHTML = '';
                    });

                    // Set consent state globally
                    window.OneTrustActiveGroups = 'C0001:1,C0002:1,C0003:1,C0004:1';
                    window.OptanonActiveGroups = 'C0001:1,C0002:1,C0003:1,C0004:1';

                    // Try to completely disable OneTrust
                    if (window.OneTrust) {
                        window.OneTrust = null;
                    }
                    if (window.Optanon) {
                        window.Optanon = null;
                    }

                    return removed;
                }
            """)

            if result > 0:
                print(f"  ✓ OneTrust modal aggressively removed ({result} elements)")
                return True
            else:
                print("  ⚠ No OneTrust elements found to remove")
        except Exception as e:
            print(f"  Aggressive removal failed: {e}")

        return False

    except Exception as e:
        print(f"  Error in enhanced OneTrust handling: {e}")
        return False


def close_promotional_popup(page) -> bool:
    """
    Check for and close any promotional or interstitial popups.
    Returns True if a popup was closed, False otherwise.
    """
    try:
        # PRIORITY: Check for InterstitialsWidget iframe modals first
        print("  Checking for InterstitialsWidget modals...")
        try:
            # Check for interstitial iframe modal pattern
            interstitial_selectors = [
                'div.overlay:has(iframe[src*="InterstitialsWidget"])',
                'div.modal:has(iframe[src*="InterstitialsWidget"])',
                'iframe[src*="InterstitialsWidget"]'
            ]

            for selector in interstitial_selectors:
                interstitial_element = page.locator(selector).first
                if interstitial_element.is_visible(timeout=300):
                    print(f"  ✓ Found InterstitialsWidget modal with selector: {selector}")

                    # Method 1: Try pressing Escape key
                    page.keyboard.press('Escape')
                    page.wait_for_timeout(500)

                    # Check if modal is still visible
                    if not interstitial_element.is_visible(timeout=200):
                        print("  ✓ InterstitialsWidget modal closed with Escape key")
                        return True

                    # Method 2: Try clicking on overlay background
                    try:
                        overlay = page.locator('div.overlay').first
                        if overlay.is_visible(timeout=200):
                            # Click on overlay (outside modal content)
                            overlay.click(position={'x': 10, 'y': 10})
                            page.wait_for_timeout(500)

                            if not interstitial_element.is_visible(timeout=200):
                                print("  ✓ InterstitialsWidget modal closed by clicking overlay")
                                return True
                    except Exception:
                        pass

                    # Method 3: Force removal via JavaScript as last resort
                    try:
                        removed = page.evaluate('''
                            () => {
                                // Find overlay containing InterstitialsWidget
                                const overlay = document.querySelector('div.overlay:has(iframe[src*="InterstitialsWidget"]), div.overlay');
                                if (overlay && overlay.querySelector('iframe[src*="InterstitialsWidget"]')) {
                                    overlay.remove();
                                    return true;
                                }
                                return false;
                            }
                        ''')

                        if removed:
                            print("  ✓ InterstitialsWidget modal removed via JavaScript")
                            page.wait_for_timeout(300)
                            return True

                    except Exception:
                        pass

                    print("  ⚠ InterstitialsWidget modal detected but could not be closed")
                    break

        except Exception as e:
            print(f"  Error checking InterstitialsWidget: {e}")

        # Multiple selectors for promotional/interstitial popups
        promo_popup_selectors = [
            # Most specific selector for this exact popup structure
            'div.paetC[role="dialog"] div.JtGqK[data-automation="interstitialClose"] button.BrOJk[aria-label="Close"]',
            'div.paetC[role="dialog"] button.BrOJk[aria-label="Close"]',
            'div.paetC[role="dialog"] button[aria-label="Close"]',
            # Alternative approaches to the same popup
            'div[role="dialog"] div.JtGqK[data-automation="interstitialClose"] button.BrOJk',
            'div.JtGqK[data-automation="interstitialClose"] button.BrOJk',
            'div[data-automation="interstitialClose"] button.BrOJk',
            'div[data-automation="interstitialClose"] button[aria-label="Close"]',
            'div.JtGqK[data-automation="interstitialClose"] button',
            # Generic but visible close buttons
            'button.BrOJk[aria-label="Close"]',
            'div[role="dialog"] button[aria-label="Close"]',
            'div[class*="interstitial"] button[aria-label="Close"]',
            # Content-based detection
            'div[role="dialog"]:has-text("Take $30 off") button[aria-label="Close"]',
            'div[role="dialog"]:has-text("Tripadvisor Rewards") button[aria-label="Close"]',
            'div[role="dialog"]:has-text("Welcome Offer") button[aria-label="Close"]',
            'div[class*="modal"]:has-text("special offer") button[aria-label="Close"]',
            'div[class*="popup"]:has-text("subscribe") button[aria-label="Close"]'
        ]

        for selector in promo_popup_selectors:
            try:
                close_btn = page.locator(selector).first
                if close_btn.is_visible(timeout=300):
                    close_btn.click()
                    print(f"  ✓ Promotional popup closed with selector: {selector}")
                    page.wait_for_timeout(300)
                    return True
            except Exception:
                continue

        # Try specific detection for TripAdvisor rewards popup
        try:
            rewards_popup = page.locator('div.paetC[role="dialog"]:has-text("Tripadvisor Rewards")').first
            if rewards_popup.is_visible(timeout=200):
                # Look for close button within this popup
                close_btn = rewards_popup.locator('button[aria-label="Close"], button.BrOJk').first
                if close_btn.is_visible(timeout=200):
                    close_btn.click()
                    print("  ✓ TripAdvisor Rewards popup closed")
                    page.wait_for_timeout(300)
                    return True
        except Exception:
            pass

        # Try direct JavaScript approach for the specific popup structure
        try:
            js_result = page.evaluate("""
                () => {
                    // Look for the specific popup structure
                    const popup = document.querySelector('div.paetC[role="dialog"]');
                    if (popup && popup.offsetWidth > 0 && popup.offsetHeight > 0) {
                        // Look for the close button within this popup
                        const closeBtn = popup.querySelector('div.JtGqK[data-automation="interstitialClose"] button.BrOJk[aria-label="Close"]');
                        if (closeBtn) {
                            closeBtn.click();
                            return 'paetC_popup_closed';
                        }

                        // Try alternative selectors within the popup
                        const altCloseBtn = popup.querySelector('button.BrOJk[aria-label="Close"]');
                        if (altCloseBtn) {
                            altCloseBtn.click();
                            return 'paetC_popup_alt_closed';
                        }

                        // Try any close button within the popup
                        const anyCloseBtn = popup.querySelector('button[aria-label="Close"]');
                        if (anyCloseBtn) {
                            anyCloseBtn.click();
                            return 'paetC_popup_any_closed';
                        }
                    }
                    return false;
                }
            """)

            if js_result:
                print(f"  ✓ Popup closed via JavaScript: {js_result}")
                page.wait_for_timeout(300)
                return True

        except Exception:
            pass

        # Try Escape key as fallback for any visible modal/popup
        try:
            modal_visible = page.locator('div[role="dialog"]:visible, div.paetC[role="dialog"]:visible').first
            if modal_visible.is_visible(timeout=100):
                page.keyboard.press('Escape')
                print("  ✓ Popup dismissed with Escape key")
                page.wait_for_timeout(200)
                return True
        except Exception:
            pass

        return False

    except Exception:
        return False


def aggressive_popup_check(page) -> bool:
    """
    More aggressive popup detection that checks for any modal-like elements.
    Used when standard popup detection might miss something.
    """
    try:
        # PRIORITY: Check for InterstitialsWidget modals first (same as in close_promotional_popup)
        try:
            interstitial_selectors = [
                'div.overlay:has(iframe[src*="InterstitialsWidget"])',
                'div.modal:has(iframe[src*="InterstitialsWidget"])',
                'iframe[src*="InterstitialsWidget"]'
            ]

            for selector in interstitial_selectors:
                interstitial_element = page.locator(selector).first
                if interstitial_element.is_visible(timeout=200):
                    print(f"  ✓ Aggressive check found InterstitialsWidget modal: {selector}")

                    # Try multiple closure methods quickly
                    page.keyboard.press('Escape')
                    page.wait_for_timeout(300)

                    if not interstitial_element.is_visible(timeout=100):
                        print("  ✓ InterstitialsWidget modal closed aggressively with Escape")
                        return True

                    # Force removal
                    try:
                        page.evaluate('''
                            () => {
                                const overlay = document.querySelector('div.overlay');
                                if (overlay && overlay.querySelector('iframe[src*="InterstitialsWidget"]')) {
                                    overlay.remove();
                                }
                            }
                        ''')
                        print("  ✓ InterstitialsWidget modal forcefully removed")
                        return True
                    except Exception:
                        pass

        except Exception:
            pass

        # Look for any dialog elements that might be popups
        dialog_selectors = [
            'div.paetC[role="dialog"]',  # Prioritize the specific popup structure
            'div[role="dialog"]',
            'div[class*="modal"]',
            'div[class*="popup"]',
            'div[class*="interstitial"]',
            'div[data-automation*="interstitial"]'
        ]

        for selector in dialog_selectors:
            try:
                dialogs = page.locator(selector)
                dialog_count = dialogs.count()

                for i in range(dialog_count):
                    dialog = dialogs.nth(i)
                    if dialog.is_visible(timeout=100):
                        # Look for close buttons within this dialog
                        close_selectors = [
                            'button[aria-label="Close"]',
                            'button.BrOJk',
                            'div[data-automation="interstitialClose"] button',
                            'button:has(svg):has-text("×")',
                            'button:has-text("×")',
                            '.close-button',
                            'button[class*="close"]'
                        ]

                        for close_selector in close_selectors:
                            try:
                                close_btn = dialog.locator(close_selector).first
                                if close_btn.is_visible(timeout=100):
                                    close_btn.click()
                                    print(f"  ✓ Aggressive popup detection: closed dialog with {close_selector}")
                                    page.wait_for_timeout(200)
                                    return True
                            except Exception:
                                continue

                        # If no close button found, try Escape
                        try:
                            page.keyboard.press('Escape')
                            print("  ✓ Aggressive popup detection: used Escape key")
                            page.wait_for_timeout(200)
                            return True
                        except Exception:
                            pass

            except Exception:
                continue

        return False

    except Exception:
        return False


def close_all_modals(page) -> bool:
    """
    Comprehensive function to close any open modals/dialogs on the page.
    Returns True if any modals were closed, False otherwise.
    """
    try:
        print("  Comprehensive modal cleanup...")

        modals_closed = 0

        # PRIORITY: Handle InterstitialsWidget modals first
        try:
            overlay_iframe = page.locator('div.overlay:has(iframe[src*="InterstitialsWidget"])').first
            if overlay_iframe.is_visible(timeout=200):
                print("  ✓ Found InterstitialsWidget overlay - attempting removal")

                # Direct JavaScript removal for comprehensive cleanup
                page.evaluate('''
                    () => {
                        const overlay = document.querySelector('div.overlay');
                        if (overlay && overlay.querySelector('iframe[src*="InterstitialsWidget"]')) {
                            overlay.remove();
                            console.log('InterstitialsWidget overlay removed');
                        }
                    }
                ''')
                modals_closed += 1
                print("  ✓ InterstitialsWidget overlay removed in comprehensive cleanup")
        except Exception:
            pass

        # Look for any dialog or modal-like elements
        modal_selectors = [
            'div[role="dialog"]',
            'div[aria-modal="true"]',
            'div[class*="modal"]:visible',
            'div[class*="popup"]:visible',
            'div[class*="overlay"]:visible',
            'div[data-automation*="modal"]:visible'
        ]

        for selector in modal_selectors:
            try:
                modals = page.locator(selector)
                modal_count = modals.count()

                for i in range(modal_count):
                    modal = modals.nth(i)
                    if modal.is_visible(timeout=200):
                        # Try to close this modal
                        close_selectors = [
                            'div.JtGqK[data-automation="interstitialClose"] button.BrOJk[aria-label="Close"]',
                            'div[data-automation="interstitialClose"] button.BrOJk',
                            'div[data-automation="interstitialClose"] button[aria-label="Close"]',
                            'div[data-automation="interstitialClose"] button',
                            'button.BrOJk[aria-label="Close"]',
                            'button[aria-label="Close"]',
                            'button[aria-label="close"]',
                            'button.BrOJk',
                            'button:has(svg):has-text("×")',
                            'button:has-text("×")',
                            'button:has-text("✕")',
                            '[role="button"][aria-label="Close"]',
                            '.close-button'
                        ]

                        modal_closed = False
                        for close_selector in close_selectors:
                            try:
                                close_btn = modal.locator(close_selector).first
                                if close_btn.is_visible(timeout=100):
                                    close_btn.click()
                                    modals_closed += 1
                                    modal_closed = True
                                    page.wait_for_timeout(200)
                                    break
                            except Exception:
                                continue

                        # If no close button, try clicking outside or Escape
                        if not modal_closed:
                            try:
                                page.keyboard.press('Escape')
                                modals_closed += 1
                                page.wait_for_timeout(200)
                            except Exception:
                                pass

            except Exception:
                continue

        if modals_closed > 0:
            print(f"  ✓ Closed {modals_closed} modals/dialogs")
            return True
        else:
            print("  No modals found to close")
            return False

    except Exception as e:
        print(f"  Error in comprehensive modal cleanup: {e}")
        return False


def send_escape_key(page, message="") -> None:
    """
    Send ESC key to close any modal dialogs that might be open.
    """
    try:
        page.keyboard.press('Escape')
        if message:
            print(f"  ✓ ESC key sent: {message}")
    except Exception:
        pass  # Silently fail if page is not ready


def extract_modal_data(page) -> Optional[Dict[str, any]]:
    """
    Extract all data from the features modal using direct keyword search.
    Assumes the modal is already open and searches for keywords directly.
    """
    try:
        modal_data = page.evaluate("""
            () => {
                const data = {};
                console.log('Starting direct keyword search (assuming modal is open)...');

                // Direct search for keywords in all elements
                const allElements = document.querySelectorAll('*');

                for (const element of allElements) {
                    const text = element.textContent?.trim();
                    if (!text) continue;

                    // CUISINES extraction
                    if (text === 'CUISINES') {
                        console.log('Found CUISINES keyword, looking for data...');

                        // Try multiple sibling/parent strategies
                        let valueElement = element.nextElementSibling ||
                                         element.parentElement?.querySelector('[class*="VImYz"]') ||
                                         element.closest('[class*="iPiKu"]')?.querySelector('[class*="VImYz"]');

                        if (valueElement?.textContent?.trim()) {
                            const cuisinesText = valueElement.textContent.trim();
                            if (cuisinesText !== 'CUISINES' && cuisinesText.length > 0) {
                                data.cuisines = cuisinesText.split(',').map(c => c.trim()).filter(c => c.length > 0);
                                console.log('✓ Found CUISINES:', data.cuisines);
                            }
                        }
                    }

                    // Meal types extraction
                    else if (text === 'Meal types') {
                        console.log('Found Meal types keyword, looking for data...');

                        let valueElement = element.nextElementSibling ||
                                         element.parentElement?.querySelector('[class*="VImYz"]') ||
                                         element.closest('[class*="iPiKu"]')?.querySelector('[class*="VImYz"]');

                        if (valueElement?.textContent?.trim()) {
                            const mealTypesText = valueElement.textContent.trim();
                            if (mealTypesText !== 'Meal types' && mealTypesText.length > 0) {
                                data.meal_types = mealTypesText.split(',').map(m => m.trim()).filter(m => m.length > 0);
                                console.log('✓ Found Meal types:', data.meal_types);
                            }
                        }
                    }

                    // Special Diets extraction
                    else if (text === 'Special Diets') {
                        console.log('Found Special Diets keyword, looking for data...');

                        let valueElement = element.nextElementSibling ||
                                         element.parentElement?.querySelector('[class*="VImYz"]') ||
                                         element.closest('[class*="iPiKu"]')?.querySelector('[class*="VImYz"]');

                        if (valueElement?.textContent?.trim()) {
                            const specialDietsText = valueElement.textContent.trim();
                            if (specialDietsText !== 'Special Diets' && specialDietsText.length > 0) {
                                data.special_diets = specialDietsText.split(',').map(d => d.trim()).filter(d => d.length > 0);
                                console.log('✓ Found Special Diets:', data.special_diets);
                            }
                        }
                    }

                    // PRICE extraction
                    else if (text === 'PRICE') {
                        console.log('Found PRICE keyword, looking for data...');

                        let valueElement = element.nextElementSibling ||
                                         element.parentElement?.querySelector('[class*="VImYz"]') ||
                                         element.closest('[class*="iPiKu"]')?.querySelector('[class*="VImYz"]');

                        if (valueElement?.textContent?.trim()) {
                            const priceText = valueElement.textContent.trim();
                            if (priceText !== 'PRICE' && priceText.length > 0) {
                                data.price = priceText;
                                console.log('✓ Found PRICE:', data.price);
                            }
                        }
                    }

                    // FEATURES extraction - find all spans after FEATURES header
                    else if (text === 'FEATURES') {
                        console.log('Found FEATURES keyword, extracting all spans...');

                        const featuresSection = element.closest('[class*="iPiKu"]') ||
                                              element.parentElement ||
                                              element.closest('div');

                        if (featuresSection) {
                            const spans = featuresSection.querySelectorAll('span');
                            const features = [];

                            spans.forEach(span => {
                                const spanText = span.textContent?.trim();
                                if (spanText &&
                                    spanText.length > 2 &&
                                    spanText.length < 100 &&
                                    spanText !== 'FEATURES' &&
                                    !features.includes(spanText)) {
                                    features.push(spanText);
                                }
                            });

                            if (features.length > 0) {
                                data.features = features;
                                console.log('✓ Found FEATURES:', features);
                            }
                        }
                    }
                }

                // Final debug output
                console.log('=== DIRECT KEYWORD EXTRACTION COMPLETE ===');
                console.log('Extracted data keys:', Object.keys(data));
                if (data.cuisines) console.log('✓ CUISINES:', data.cuisines);
                if (data.meal_types) console.log('✓ MEAL TYPES:', data.meal_types);
                if (data.special_diets) console.log('✓ SPECIAL DIETS:', data.special_diets);
                if (data.price) console.log('✓ PRICE:', data.price);
                if (data.features) console.log('✓ FEATURES:', data.features);

                return Object.keys(data).length > 0 ? data : null;
            }
        """)

        return modal_data

    except Exception as e:
        print(f"  Error extracting modal data: {e}")
        return None


def extract_hours(page) -> Optional[Dict[str, str]]:
    """
    Extract restaurant hours from the page.
    Returns a dictionary with days as keys and hours as values.
    """
    try:
        hours = page.evaluate("""
            () => {
                // Find the hours section
                const hoursSection = document.querySelector('[data-automation="hours-section"]');
                if (!hoursSection) {
                    // Alternative: look for "Hours" text and find parent section
                    const allDivs = document.querySelectorAll('div');
                    for (const div of allDivs) {
                        if (div.textContent === 'Hours') {
                            const parent = div.closest('[class*="f e"]');
                            if (parent) {
                                hoursSection = parent;
                                break;
                            }
                        }
                    }
                }

                if (!hoursSection) {
                    return null;
                }

                const hoursData = {};
                const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

                // Look for each day and its hours
                for (const day of days) {
                    // Find div containing the day name
                    const dayElements = hoursSection.querySelectorAll('div');
                    for (let i = 0; i < dayElements.length; i++) {
                        const elem = dayElements[i];
                        if (elem.textContent.trim() === day) {
                            // Look for the hours in nearby elements
                            let hoursElem = elem.parentElement?.nextElementSibling;
                            if (!hoursElem) {
                                // Try looking in parent's parent structure
                                const parent = elem.closest('.f');
                                if (parent) {
                                    hoursElem = parent.querySelector('span');
                                }
                            }

                            if (hoursElem) {
                                const hoursText = hoursElem.textContent.trim();
                                // Check if it looks like hours (contains AM/PM or numbers with colon)
                                if (hoursText && (hoursText.includes('AM') || hoursText.includes('PM') ||
                                    hoursText.includes('Closed') || hoursText.match(/\\d+:\\d+/))) {
                                    hoursData[day] = hoursText;
                                }
                            }
                        }
                    }
                }

                // Also try to extract current status (e.g., "Closed now • Opens at 12:00 PM")
                const statusElements = hoursSection.querySelectorAll('div');
                for (const elem of statusElements) {
                    const text = elem.textContent;
                    if (text && (text.includes('Closed now') || text.includes('Open now'))) {
                        hoursData['current_status'] = text.trim();
                        break;
                    }
                }

                return Object.keys(hoursData).length > 0 ? hoursData : null;
            }
        """)

        if hours:
            print(f"  Found hours for {len(hours)} days")
            if 'current_status' in hours:
                print(f"    Current status: {hours['current_status']}")
            return hours

    except Exception as e:
        print(f"  Error extracting hours: {e}")

    return None


def extract_special_diets(page) -> Optional[List[str]]:
    """
    Extract special diets from the restaurant page.
    Searches for "Special Diets" text and extracts the diet options list.
    """
    try:
        special_diets = page.evaluate("""
            () => {
                // Find all div elements that contain "Special Diets" text
                const allDivs = document.querySelectorAll('div');

                for (const div of allDivs) {
                    // Check if this div contains "Special Diets"
                    if (div.textContent === 'Special Diets') {
                        // Look for sibling or nearby div containing the actual diet options
                        const parent = div.parentElement;
                        if (parent) {
                            // Look for the next div that contains the diet list
                            const divs = parent.querySelectorAll('div');
                            for (const siblingDiv of divs) {
                                // Skip the label div
                                if (siblingDiv.textContent !== 'Special Diets' &&
                                    siblingDiv.textContent &&
                                    siblingDiv.textContent.includes(',')) {
                                    // Found diet list - split by comma and clean up
                                    const dietText = siblingDiv.textContent.trim();
                                    const diets = dietText.split(',').map(d => d.trim()).filter(d => d);
                                    if (diets.length > 0) {
                                        return diets;
                                    }
                                }
                            }
                        }
                    }
                }

                // Alternative approach: look for divs that look like special diet lists
                const possibleDietElements = document.querySelectorAll('div');
                for (const elem of possibleDietElements) {
                    const text = elem.textContent;
                    // Check if it looks like a special diet list
                    if (text &&
                        text.includes(',') &&
                        text.length < 200 && // Diet lists are typically short
                        (text.includes('Vegetarian') || text.includes('Vegan') ||
                         text.includes('Gluten') || text.includes('Halal') ||
                         text.includes('Kosher') || text.includes('Dairy') ||
                         text.includes('Lactose') || text.includes('Nut') ||
                         text.includes('friendly') || text.includes('free options'))) {
                        // Check if previous sibling or nearby element says "Special Diets"
                        const prev = elem.previousElementSibling;
                        if (prev && prev.textContent && prev.textContent.includes('Special Diets')) {
                            const diets = text.split(',').map(d => d.trim()).filter(d => d);
                            if (diets.length > 0) {
                                return diets;
                            }
                        }
                    }
                }

                // Fallback: look for common diet-related text patterns
                const dietKeywords = [
                    'Vegetarian friendly',
                    'Vegan options',
                    'Gluten free options',
                    'Halal',
                    'Kosher',
                    'Dairy free options',
                    'Lactose free options',
                    'Nut free options'
                ];

                const foundDiets = [];
                const allElements = document.querySelectorAll('div, span');

                for (const element of allElements) {
                    const text = element.textContent.trim();
                    for (const keyword of dietKeywords) {
                        if (text.includes(keyword) && !foundDiets.includes(keyword)) {
                            // Check if this is likely in a special diets section
                            const parent = element.parentElement;
                            if (parent) {
                                const parentText = parent.textContent;
                                if (parentText.includes('Special Diets') ||
                                    parentText.includes('Dietary') ||
                                    dietKeywords.filter(k => parentText.includes(k)).length > 1) {
                                    foundDiets.push(keyword);
                                }
                            }
                        }
                    }
                }

                if (foundDiets.length > 0) {
                    return foundDiets;
                }

                return null;
            }
        """)

        if special_diets:
            print(f"  Found special diets: {', '.join(special_diets)}")
            return special_diets

    except Exception as e:
        print(f"  Error extracting special diets: {e}")

    return None


def extract_features(page) -> Optional[List[str]]:
    """
    Extract features from the restaurant page.
    Searches for FEATURES text and extracts the feature list with enhanced detection.
    """
    try:
        features = page.evaluate("""
            () => {
                // Strategy 1: Find the FEATURES section using multiple approaches
                const allElements = document.querySelectorAll('div, span, h3, h4');
                let featuresContainer = null;
                let features = [];

                // Look for FEATURES text in various elements
                for (const element of allElements) {
                    const text = element.textContent?.trim();
                    if (text === 'FEATURES' || text === 'Features' || text === 'features') {
                        // Look for the parent container that has the features list
                        let parent = element.parentElement;
                        let searchDepth = 0;

                        while (parent && searchDepth < 5) {
                            // Look for container with multiple feature items
                            const featureElements = parent.querySelectorAll('span, div, li');
                            const potentialFeatures = [];

                            for (const el of featureElements) {
                                const elText = el.textContent?.trim();
                                if (elText &&
                                    elText !== 'FEATURES' &&
                                    elText !== 'Features' &&
                                    elText !== 'features' &&
                                    elText.length > 2 &&
                                    elText.length < 100 &&
                                    !elText.includes('See all') &&
                                    !elText.includes('More info')) {

                                    // Check if this element contains multiple features concatenated
                                    // Common patterns: "Lunch, Dinner" or "SeatingWiFi" etc.
                                    let features = [elText];

                                    // Split on common separators but only if it creates meaningful parts
                                    if (elText.includes(', ')) {
                                        const parts = elText.split(', ').map(p => p.trim()).filter(p => p.length > 1);
                                        if (parts.length > 1 && parts.every(p => p.length < 30)) {
                                            features = parts;
                                        }
                                    }
                                    // Handle cases like "LunchDinner" or "SeatingWiFi"
                                    else if (elText.length > 15 && /[a-z][A-Z]/.test(elText)) {
                                        // Split on camelCase boundaries but only if result makes sense
                                        const parts = elText.split(/(?=[A-Z])/).filter(p => p.length > 2);
                                        if (parts.length > 1 && parts.length < 5) {
                                            features = parts;
                                        }
                                    }

                                    potentialFeatures.push(...features);
                                }
                            }

                            if (potentialFeatures.length >= 2) {
                                featuresContainer = parent;
                                features = potentialFeatures;
                                break;
                            }
                            parent = parent.parentElement;
                            searchDepth++;
                        }
                        if (featuresContainer) break;
                    }
                }

                // Strategy 2: If we found a features container, do more refined extraction
                if (featuresContainer && features.length > 0) {
                    // Remove duplicates while preserving order
                    const uniqueFeatures = [];
                    const seen = new Set();

                    for (const feature of features) {
                        if (!seen.has(feature.toLowerCase())) {
                            uniqueFeatures.push(feature);
                            seen.add(feature.toLowerCase());
                        }
                    }

                    // Filter out obviously wrong items (too long, contains numbers suggesting counts, etc.)
                    const cleanFeatures = uniqueFeatures.filter(f => {
                        const isNotCount = !/^\\d+$/.test(f);
                        const isNotTooLong = f.length < 50;
                        const isNotEmpty = f.length > 1;
                        const isNotCommonNoise = !['see all', 'more', 'less', 'show all', 'hide'].some(noise =>
                            f.toLowerCase().includes(noise)
                        );
                        return isNotCount && isNotTooLong && isNotEmpty && isNotCommonNoise;
                    });

                    if (cleanFeatures.length > 0) {
                        console.log('Features found via strategy 1:', cleanFeatures);
                        return cleanFeatures;
                    }
                }

                // Strategy 1.5: Direct sibling approach - look for features as direct siblings
                console.log('Trying strategy 1.5: direct sibling approach');
                for (const element of allElements) {
                    const text = element.textContent?.trim();
                    if (text === 'FEATURES' || text === 'Features' || text === 'features') {
                        // Look at direct siblings that might be individual features
                        let current = element.nextElementSibling;
                        const siblingFeatures = [];
                        let scanCount = 0;

                        while (current && scanCount < 20) {
                            const siblingText = current.textContent?.trim();
                            if (siblingText &&
                                siblingText.length > 2 &&
                                siblingText.length < 50 &&
                                !siblingText.toLowerCase().includes('see all') &&
                                !siblingText.toLowerCase().includes('more') &&
                                !siblingText.includes('FEATURES')) {

                                // Check if this looks like a feature
                                if (siblingText.includes('Seating') ||
                                    siblingText.includes('Accessible') ||
                                    siblingText.includes('WiFi') ||
                                    siblingText.includes('Parking') ||
                                    siblingText.includes('Bar') ||
                                    siblingText.includes('Cards') ||
                                    siblingText.includes('Reservations') ||
                                    /^[A-Z][a-z]/.test(siblingText)) {  // Starts with capital letter

                                    siblingFeatures.push(siblingText);
                                }
                            }
                            current = current.nextElementSibling;
                            scanCount++;
                        }

                        if (siblingFeatures.length >= 1) {
                            console.log('Features found via strategy 1.5 (siblings):', siblingFeatures);
                            return siblingFeatures;
                        }
                        break; // Found FEATURES heading, don't look for more
                    }
                }

                // Strategy 2: Look for TripAdvisor-specific feature structures
                console.log('Trying strategy 2: TripAdvisor-specific structures');

                // Look for common TripAdvisor feature container patterns
                const featureContainerSelectors = [
                    'div[data-automation*="feature"]',
                    'div[class*="feature"]',
                    'div[class*="amenities"]',
                    'div[class*="services"]',
                    'ul[class*="feature"]'
                ];

                // Add explicit search for individual feature spans
                featureContainerSelectors.push('body'); // Fallback to search entire body

                for (const selector of featureContainerSelectors) {
                    try {
                        const containers = document.querySelectorAll(selector);
                        for (const container of containers) {
                            const containerFeatures = [];
                            const items = container.querySelectorAll('span, div, li');

                            for (const item of items) {
                                const itemText = item.textContent?.trim();
                                if (itemText &&
                                    itemText.length > 2 &&
                                    itemText.length < 60 &&
                                    !itemText.toLowerCase().includes('see all') &&
                                    !itemText.toLowerCase().includes('more info')) {

                                    // Split concatenated features if needed
                                    let features = [itemText];
                                    if (itemText.includes(', ')) {
                                        const parts = itemText.split(', ').map(p => p.trim());
                                        if (parts.length > 1) features = parts;
                                    }

                                    containerFeatures.push(...features);
                                }
                            }

                            if (containerFeatures.length >= 2) {
                                console.log('Features found via strategy 2 (TripAdvisor structures):', containerFeatures);
                                return [...new Set(containerFeatures)]; // Remove duplicates
                            }
                        }
                    } catch (e) {
                        continue;
                    }
                }

                // Strategy 3: Look for feature-like patterns and common features
                console.log('Trying strategy 3: pattern-based feature detection');

                // Expanded feature keywords including common ones that might be missed
                const featureKeywords = [
                    'Accepts Credit Cards', 'Full Bar', 'Gift Cards Available', 'Highchairs Available',
                    'Outdoor Seating', 'Reservations', 'Seating', 'Serves Alcohol', 'Table Service',
                    'Wheelchair Accessible', 'Free Wifi', 'Parking Available', 'Valet Parking',
                    'Television', 'Live Music', 'Private Dining', 'Delivery', 'Takeout', 'Drive Thru',
                    'Air Conditioning', 'Bar', 'Buffet', 'Catering', 'Counter Service', 'Family Style',
                    'Happy Hour', 'Kids Menu', 'Non-smoking', 'Pet Friendly', 'Rooftop', 'Sports Bar',
                    'Terrace', 'Waterfront', 'Wine List', 'Breakfast', 'Brunch', 'Dinner', 'Lunch',
                    'Late Night', 'Group Meals', 'Special Occasions', 'Business Meals', 'Groups',
                    'Romantic', 'Families with children'
                ];

                // Look for elements that contain feature-like text
                const foundFeatures = [];
                const allTextElements = document.querySelectorAll('div, span, li, p');
                const seenFeatures = new Set();

                for (const element of allTextElements) {
                    const text = element.textContent?.trim();

                    if (!text || text.length < 3 || text.length > 50) continue;

                    // Check against known keywords
                    for (const keyword of featureKeywords) {
                        if (text === keyword && !seenFeatures.has(keyword.toLowerCase())) {
                            // Verify this looks like it's in a features context
                            const parent = element.parentElement;
                            if (parent) {
                                const parentText = parent.textContent?.toLowerCase() || '';
                                const contextualClues = ['feature', 'amenity', 'service', 'dining', 'payment'];
                                const hasContext = contextualClues.some(clue => parentText.includes(clue));

                                // Check for nearby feature-like siblings
                                const siblings = Array.from(parent.querySelectorAll('span, div, li'))
                                    .map(el => el.textContent?.trim())
                                    .filter(t => t && t.length > 2);

                                const nearbyFeatures = siblings.filter(t =>
                                    featureKeywords.some(k => t === k)
                                ).length;

                                if (nearbyFeatures >= 1 || hasContext) {
                                    foundFeatures.push(keyword);
                                    seenFeatures.add(keyword.toLowerCase());
                                }
                            }
                        }
                    }

                    // Also capture potential features that look like amenities/services but aren't in our keyword list
                    if (text.length < 40 &&
                        (text.includes('Available') || text.includes('Service') || text.includes('Friendly') ||
                         text.includes('Seating') || text.includes('Parking') || text.includes('Menu') ||
                         text.includes('Bar') || text.includes('Wifi') || text.includes('Cards'))) {

                        // Make sure it looks feature-like and isn't already captured
                        if (!seenFeatures.has(text.toLowerCase()) &&
                            !text.toLowerCase().includes('see all') &&
                            !text.toLowerCase().includes('more info') &&
                            !/\\d{4}/.test(text)) { // No years

                            const parent = element.parentElement;
                            if (parent) {
                                const siblings = Array.from(parent.querySelectorAll('span, div, li'))
                                    .map(el => el.textContent?.trim())
                                    .filter(t => t && t.length > 2);

                                // If there are multiple similar-looking items, it's likely a features list
                                if (siblings.length >= 2) {
                                    foundFeatures.push(text);
                                    seenFeatures.add(text.toLowerCase());
                                }
                            }
                        }
                    }
                }

                if (foundFeatures.length > 0) {
                    console.log('Features found via strategy 3:', foundFeatures);
                    return foundFeatures;
                }

                // Strategy 4: Aggressive individual element scan
                console.log('Trying strategy 4: aggressive individual element scan');
                const aggressiveFeatures = [];
                const aggressiveKeywords = [
                    'Outdoor Seating', 'Indoor Seating', 'Wheelchair Accessible', 'Reservations',
                    'Free Wifi', 'Parking Available', 'Valet Parking', 'Street Parking',
                    'Full Bar', 'Wine List', 'Happy Hour', 'Serves Alcohol',
                    'Accepts Credit Cards', 'Cash Only', 'Digital Payments',
                    'Delivery', 'Takeout', 'Curbside Pickup', 'Drive Through',
                    'Kid Friendly', 'High Chairs', 'Changing Table',
                    'Pet Friendly', 'Dog Friendly', 'Outdoor Dog Area',
                    'Television', 'Live Music', 'Karaoke', 'Private Dining',
                    'Group Dining', 'Business Meetings', 'Romantic',
                    'Lunch', 'Dinner', 'Breakfast', 'Brunch', 'Late Night',
                    'Buffet', 'All You Can Eat', 'Table Service', 'Counter Service'
                ];

                // Scan all elements for exact matches to known features
                for (const element of document.querySelectorAll('span, div, li, td')) {
                    const text = element.textContent?.trim();
                    if (text) {
                        for (const keyword of aggressiveKeywords) {
                            if (text === keyword && !aggressiveFeatures.includes(keyword)) {
                                // Double-check this isn't just navigation or unwanted text
                                const elementRect = element.getBoundingClientRect();
                                if (elementRect.width > 0 && elementRect.height > 0) { // Element is visible
                                    aggressiveFeatures.push(keyword);
                                }
                                break;
                            }
                        }
                    }
                }

                if (aggressiveFeatures.length > 0) {
                    console.log('Features found via strategy 4 (aggressive scan):', aggressiveFeatures);
                    return aggressiveFeatures;
                }

                return null;
            }
        """)

        if features:
            print(f"  ✓ Found {len(features)} features: {', '.join(features[:5])}" +
                  ("..." if len(features) > 5 else ""))
            # Log all features for debugging
            if len(features) <= 10:
                print(f"    All features: {features}")
            return features
        else:
            print("  No features detected by any extraction strategy")

    except Exception as e:
        print(f"  Error extracting features: {e}")

    print("  Features extraction returned None")
    return None


def extract_meal_types(page) -> Optional[List[str]]:
    """
    Extract meal types from the restaurant page.
    Searches for "Meal types" or "MEALS" text and extracts the meal type list.
    """
    try:
        meal_types = page.evaluate("""
            () => {
                // Find all div elements that contain "Meal types" or "MEALS" text
                const allDivs = document.querySelectorAll('div');

                for (const div of allDivs) {
                    // Check if this div contains "Meal types" or "MEALS"
                    if (div.textContent === 'Meal types' || div.textContent === 'MEALS') {
                        // Look for sibling or nearby div containing the actual meal types
                        const parent = div.parentElement;
                        if (parent) {
                            // Look for the next div that contains the meal type list
                            const divs = parent.querySelectorAll('div');
                            for (const siblingDiv of divs) {
                                // Skip the label div
                                if (siblingDiv.textContent !== 'Meal types' &&
                                    siblingDiv.textContent !== 'MEALS' &&
                                    siblingDiv.textContent &&
                                    siblingDiv.textContent.includes(',')) {
                                    // Found meal type list - split by comma and clean up
                                    const mealText = siblingDiv.textContent.trim();
                                    const meals = mealText.split(',').map(m => m.trim()).filter(m => m);
                                    if (meals.length > 0) {
                                        return meals;
                                    }
                                }
                            }
                        }
                    }
                }

                // Alternative approach: look for divs that look like meal type lists
                const possibleMealElements = document.querySelectorAll('div');
                for (const elem of possibleMealElements) {
                    const text = elem.textContent;
                    // Check if it looks like a meal type list
                    if (text &&
                        text.includes(',') &&
                        text.length < 200 && // Meal type lists are typically short
                        (text.includes('Breakfast') || text.includes('Lunch') ||
                         text.includes('Dinner') || text.includes('Brunch') ||
                         text.includes('Drinks') || text.includes('Late Night') ||
                         text.includes('Dessert') || text.includes('Coffee'))) {
                        // Check if previous sibling or nearby element says "Meal types" or "MEALS"
                        const prev = elem.previousElementSibling;
                        if (prev && prev.textContent &&
                            (prev.textContent.includes('Meal types') || prev.textContent.includes('MEALS'))) {
                            const meals = text.split(',').map(m => m.trim()).filter(m => m);
                            if (meals.length > 0) {
                                return meals;
                            }
                        }
                    }
                }

                return null;
            }
        """)

        if meal_types:
            print(f"  Found meal types: {', '.join(meal_types)}")
            return meal_types

    except Exception as e:
        print(f"  Error extracting meal types: {e}")

    return None


def extract_cuisines(page) -> Optional[List[str]]:
    """
    Extract cuisines from the restaurant page.
    Searches for CUISINES text and extracts the cuisine list.
    """
    try:
        cuisines = page.evaluate("""
            () => {
                // Find all div elements that contain "CUISINES" text
                const allDivs = document.querySelectorAll('div');

                for (const div of allDivs) {
                    // Check if this div contains "CUISINES"
                    if (div.textContent === 'CUISINES') {
                        // Look for sibling or nearby div containing the actual cuisines
                        const parent = div.parentElement;
                        if (parent) {
                            // Look for the next div that contains the cuisine list
                            const divs = parent.querySelectorAll('div');
                            for (const siblingDiv of divs) {
                                // Skip the CUISINES label div
                                if (siblingDiv.textContent !== 'CUISINES' &&
                                    siblingDiv.textContent &&
                                    siblingDiv.textContent.includes(',')) {
                                    // Found cuisine list - split by comma and clean up
                                    const cuisineText = siblingDiv.textContent.trim();
                                    const cuisines = cuisineText.split(',').map(c => c.trim()).filter(c => c);
                                    if (cuisines.length > 0) {
                                        return cuisines;
                                    }
                                }
                            }
                        }
                    }
                }

                // Alternative approach: look for specific patterns
                const patterns = [
                    // Look for divs with text that looks like cuisine lists
                    'div:has-text("Dutch, European")',
                    'div:has-text("Italian, Pizza")',
                    'div:has-text("Asian, Thai")',
                    'div:has-text("American, Bar")'
                ];

                // Try to find any div that looks like a cuisine list
                const possibleCuisineElements = document.querySelectorAll('div');
                for (const elem of possibleCuisineElements) {
                    const text = elem.textContent;
                    // Check if it looks like a cuisine list (contains commas and typical cuisine words)
                    if (text &&
                        text.includes(',') &&
                        text.length < 200 && // Cuisine lists are typically short
                        (text.includes('European') || text.includes('Asian') ||
                         text.includes('American') || text.includes('Italian') ||
                         text.includes('French') || text.includes('Chinese') ||
                         text.includes('Japanese') || text.includes('Mexican') ||
                         text.includes('Indian') || text.includes('Thai') ||
                         text.includes('Mediterranean') || text.includes('Dutch') ||
                         text.includes('Pub') || text.includes('Bar') ||
                         text.includes('Seafood') || text.includes('Steakhouse'))) {
                        // Check if previous sibling or nearby element says CUISINES
                        const prev = elem.previousElementSibling;
                        if (prev && prev.textContent && prev.textContent.includes('CUISINES')) {
                            const cuisines = text.split(',').map(c => c.trim()).filter(c => c);
                            if (cuisines.length > 0) {
                                return cuisines;
                            }
                        }
                    }
                }

                return null;
            }
        """)

        if cuisines:
            print(f"  Found cuisines: {', '.join(cuisines)}")
            return cuisines

    except Exception as e:
        print(f"  Error extracting cuisines: {e}")

    return None


def extract_phone_number(page) -> Optional[str]:
    """
    Extract the restaurant's phone number from the page.
    Looks for tel: links.
    """
    try:
        phone_number = page.evaluate("""
            () => {
                // Look for phone links
                const phoneLinks = document.querySelectorAll('a[href^="tel:"]');

                for (const link of phoneLinks) {
                    if (link.href && link.href.startsWith('tel:')) {
                        // Extract phone number from href
                        const phone = link.href.replace('tel:', '');
                        return phone;
                    }
                }

                return null;
            }
        """)

        if phone_number:
            print(f"  Found phone number: {phone_number}")
            return phone_number

    except Exception as e:
        print(f"  Error extracting phone number: {e}")

    return None


def extract_restaurant_website(page) -> Optional[str]:
    """
    Extract the restaurant's actual website URL from the page.
    Uses the specific data-automation attribute for more reliable extraction.
    """
    try:
        website_url = page.evaluate("""
            () => {
                // First try the specific restaurant website button selector
                const websiteButton = document.querySelector('a[data-automation="restaurantsWebsiteButton"]');
                if (websiteButton && websiteButton.href && !websiteButton.href.includes('tripadvisor.com')) {
                    return websiteButton.href;
                }

                // Fallback to other website link patterns
                const websiteSelectors = [
                    'a[href*="http"]:has-text("Website")',
                    'a[href*="http"]:has-text("Visit website")',
                    'a[href*="http"]:has-text("Official website")',
                    'a[data-test-target*="website"]',
                    'a[href*="http"][title*="website"]',
                    'a[href*="http"][title*="Website"]'
                ];

                for (const selector of websiteSelectors) {
                    const element = document.querySelector(selector);
                    if (element && element.href && !element.href.includes('tripadvisor.com')) {
                        return element.href;
                    }
                }

                // Look for any external links in contact/info sections
                const sections = document.querySelectorAll('[class*="contact"], [class*="info"], [class*="detail"]');
                for (const section of sections) {
                    const links = section.querySelectorAll('a[href*="http"]');
                    for (const link of links) {
                        if (link.href &&
                            !link.href.includes('tripadvisor.com') &&
                            !link.href.includes('facebook.com') &&
                            !link.href.includes('instagram.com') &&
                            !link.href.includes('twitter.com')) {
                            return link.href;
                        }
                    }
                }

                return null;
            }
        """)

        if website_url:
            print(f"  Found restaurant website: {website_url}")
            return website_url

    except Exception as e:
        print(f"  Error extracting website: {e}")

    return None


def extract_restaurant_jsonld(page) -> Optional[Dict[str, Any]]:
    """
    Extract JSON-LD structured data from restaurant detail page.
    Look for script elements with FoodEstablishment data.
    """
    try:
        # Execute JavaScript to extract the JSON-LD content
        jsonld_content = page.evaluate("""
            () => {
                // Look for all JSON-LD scripts
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');

                for (const script of scripts) {
                    try {
                        const content = script.textContent;
                        if (content) {
                            const parsed = JSON.parse(content);
                            // Check if this is a FoodEstablishment
                            if (parsed['@type'] === 'FoodEstablishment') {
                                return content;
                            }
                        }
                    } catch (e) {
                        // Skip invalid JSON scripts
                        continue;
                    }
                }

                return null;
            }
        """)

        if jsonld_content:
            # Parse the JSON-LD content
            jsonld_data = json.loads(jsonld_content)

            # Extract relevant information
            restaurant_data = {
                'name': jsonld_data.get('name', ''),
                'image': jsonld_data.get('image', []),
                'priceRange': jsonld_data.get('priceRange', ''),
                'url': jsonld_data.get('url', ''),
                'website': jsonld_data.get('url', ''),  # Store website link
                'geo': jsonld_data.get('geo', {}),
                'address': jsonld_data.get('address', {}),
                'aggregateRating': jsonld_data.get('aggregateRating', {}),
                'raw_jsonld': jsonld_data  # Keep the full data
            }

            print(f"  Extracted JSON-LD data for: {restaurant_data['name']}")
            if restaurant_data.get('aggregateRating'):
                rating = restaurant_data['aggregateRating']
                print(f"    Rating: {rating.get('ratingValue')} ({rating.get('reviewCount')} reviews)")
            if restaurant_data.get('address'):
                addr = restaurant_data['address']
                street = addr.get('streetAddress', '')
                postal = addr.get('postalCode', '')
                locality = addr.get('addressLocality', '')
                print(f"    Address: {street}, {postal} {locality}")

            return restaurant_data

    except Exception as e:
        print(f"  Error extracting JSON-LD data: {e}")

    return None


def update_restaurant_last_scraped(restaurant_id: int, status: str = "completed"):
    """
    Update the last_scraped timestamp for a restaurant.
    """
    try:
        # Prepare the update data
        update_data = {
            "last_scraped": datetime.now().isoformat()
        }

        response = requests.put(
            f"https://viberoam.ai/api/restaurants/{restaurant_id}/",
            json=update_data,
            headers={"Content-Type": "application/json"},
            verify=False
        )

        if response.status_code == 200:
            print(f"  Updated last_scraped for restaurant {restaurant_id}")
            return True
        else:
            print(f"  Error updating restaurant {restaurant_id}: {response.status_code}")
            try:
                error_detail = response.json()
                print(f"    Error details: {error_detail}")
            except:
                print(f"    Response text: {response.text}")
            return False

    except Exception as e:
        print(f"  Exception updating restaurant {restaurant_id}: {e}")
        return False


def get_restaurant_links(country="AT"):
    """Fetch a single random restaurant that hasn't been scraped yet.

    Args:
        country: Two-letter country code (e.g., "AT", "NL", "IT", "ES"). Defaults to "AT".
    """
    try:
        response = requests.get(
            f"https://viberoam.ai/api/restaurants/random/?country={country}&never_scraped=1",
            timeout=30,
            verify=False
        )

        if response.status_code == 200:
            restaurant = response.json()
            # Return as a list with one restaurant to maintain compatibility
            return [restaurant]
        else:
            print(f"Error fetching restaurant: {response.status_code}")
            return []

    except Exception as e:
        print(f"Error fetching restaurant: {e}")
        return []


def run_browser_scraping_in_thread(restaurant):
    """Run browser scraping in a separate thread to avoid asyncio conflicts."""
    # Check if we're in an asyncio event loop
    try:
        asyncio.get_running_loop()
        # If we get here, we're in an asyncio loop - need to run in thread
        result_container = {}
        thread = threading.Thread(target=_do_browser_scraping, args=(restaurant, result_container))
        thread.start()
        thread.join()
        return result_container
    except RuntimeError:
        # No asyncio loop - can run directly
        result_container = {}
        _do_browser_scraping(restaurant, result_container)
        return result_container


def _do_browser_scraping(restaurant, result_container):
    """Actual browser scraping logic - runs outside asyncio loop."""
    # Initialize data collection variables
    graphql_responses: List[Dict[str, Any]] = []
    jsonld_data = None
    scrape_status = "started"
    errors = []

    try:
        with Camoufox(
            headless=True,
        ) as browser:
                page = browser.new_page()
                # Set up response interceptor for GraphQL endpoints

                def handle_response(response):
                    # Check if this is a GraphQL endpoint
                    url_lower = response.url.lower()
                    if 'graphql' in url_lower or '/data/graphql' in url_lower:
                        try:
                            # Try to get JSON response
                            response_data = response.json()

                            # Store the response data
                            graphql_responses.append({
                                'url': response.url,
                                'status': response.status,
                                'data': response_data,
                                'timestamp': len(graphql_responses)
                            })

                            print(
                                f"  Captured GraphQL response "
                                f"#{len(graphql_responses)}"
                            )
                            print(f"    URL: {response.url[:80]}...")
                            print(f"    Status: {response.status}")

                            # Print a sample of the data structure
                            if response_data:
                                if isinstance(response_data, dict):
                                    data_keys = list(response_data.keys())
                                else:
                                    data_keys = type(response_data).__name__
                                print(f"    Data keys: {data_keys}")

                        except Exception as e:
                            print(f"  Error parsing GraphQL response: {e}")

                # Attach the response handler
                page.on('response', handle_response)

                # Navigate to the page
                try:
                    print(f"Navigating to: {restaurant['tripadvisor_detail_page']}")

                    # Enhanced OneTrust bypass - set cookies before navigation
                    print("  Setting up enhanced OneTrust bypass...")
                    config = detect_onetrust_config(page)
                    set_onetrust_cookies(page, config)

                    page.goto(restaurant['tripadvisor_detail_page'], wait_until='networkidle', timeout=30000)

                    # Send ESC key immediately after page load to close any modals
                    send_escape_key(page, "after page load")

                    # Post-navigation OneTrust handling if modal still appears
                    handle_onetrust_modal_enhanced(page)

                    # Send ESC key again after OneTrust handling
                    send_escape_key(page, "after OneTrust handling")

                    # Check for promotional popups immediately after page load
                    if not close_promotional_popup(page):
                        # If standard detection missed it, try aggressive detection
                        aggressive_popup_check(page)

                    # Removed old OneTrust handling code - now using enhanced handler above

                    # Enhanced language modal detection and handling
                    try:
                        print("  Checking for language selection modals...")

                        # Multiple selectors for language modals that might appear
                        language_modal_selectors = [
                            '[data-automation="languageSelection"]',
                            'div[role="dialog"]:has-text("language")',
                            'div[role="dialog"]:has-text("Language")',
                            'div[class*="language"]:has(button)',
                            'div[class*="Language"]:has(button)',
                            'div[class*="locale"]:has(button)',
                            '[aria-label*="language"]',
                            '[aria-label*="Language"]'
                        ]

                        language_modal_found = False
                        for selector in language_modal_selectors:
                            try:
                                modal = page.locator(selector).first
                                if modal.is_visible(timeout=800):
                                    print(f"  Language modal detected with selector: {selector}")

                                    # Try various close button strategies
                                    close_selectors = [
                                        'button[aria-label="Close"]',
                                        'button:has-text("×")',
                                        'button:has-text("✕")',
                                        'button[class*="close"]',
                                        'button[data-automation="close"]',
                                        '[role="button"]:has-text("×")',
                                        '.close-button',
                                        'button:has(svg[class*="close"])'
                                    ]

                                    close_clicked = False
                                    for close_selector in close_selectors:
                                        try:
                                            close_btn = modal.locator(close_selector).first
                                            if close_btn.is_visible(timeout=300):
                                                close_btn.click()
                                                print(f"  ✓ Language modal closed with: {close_selector}")
                                                close_clicked = True
                                                break
                                        except Exception:
                                            continue

                                    # If no close button found, try pressing Escape
                                    if not close_clicked:
                                        page.keyboard.press('Escape')
                                        print("  ✓ Language modal dismissed with Escape key")

                                    language_modal_found = True
                                    page.wait_for_timeout(500)  # Brief wait after closing
                                    break

                            except Exception:
                                continue

                        if not language_modal_found:
                            print("  No language modal detected")

                    except Exception as e:
                        print(f"  Error handling language modal: {e}")

                    # Check for and close promotional/interstitial popups
                    print("  Checking for promotional popups...")
                    if not close_promotional_popup(page):
                        # Try aggressive detection if standard method didn't find anything
                        if not aggressive_popup_check(page):
                            print("  No promotional popups detected")

                    # Wait a bit for initial content to load
                    page.wait_for_timeout(2000)

                    # Try to click "Clear all filters" button first
                    try:
                        print("  Looking for 'Clear all filters' button...")
                        # Look for button containing "Clear all filters" text
                        clear_filters_button = page.locator('button:has-text("Clear all filters")')

                        if clear_filters_button.count() > 0:
                            print("  Clicking 'Clear all filters' button...")
                            clear_filters_button.first.click()

                            # Wait for network to be idle after clicking
                            try:
                                page.wait_for_load_state('networkidle', timeout=10000)
                            except Exception:
                                pass  # Continue even if networkidle times out

                            print("  Successfully clicked 'Clear all filters' button")
                        else:
                            print("  'Clear all filters' button not found")

                    except Exception as e:
                        print(f"  Cookie/privacy banner handling error: {e}")

                    # Change review language filter from English to "All languages"
                    try:
                        print("  Checking review language filter...")

                        # Look for language filter dropdown
                        language_filter_selectors = [
                            'div[data-automation="ugcLanguageFilter"] button',
                            'button[aria-label*="Language"]:has-text("English")',
                            'button.Datwj:has-text("English")',
                            'div[data-automation="ugcLanguageFilter"] button.Datwj'
                        ]

                        language_filter_found = False
                        for selector in language_filter_selectors:
                            try:
                                filter_button = page.locator(selector).first
                                if filter_button.is_visible(timeout=1000):
                                    print("  Found language filter, clicking to open...")
                                    filter_button.click()
                                    page.wait_for_timeout(500)  # Wait for dropdown to open

                                    # Look for "All languages" option
                                    all_lang_selectors = [
                                        'span:has-text("All languages")',
                                        '[data-automation="ugcLanguageFilterOption_0"]',
                                        'div[role="option"]:has-text("All languages")',
                                        '#menu-item-allLang',
                                        'span[data-testid="menuitem"]:has-text("All languages")'
                                    ]

                                    all_lang_clicked = False
                                    for all_lang_selector in all_lang_selectors:
                                        try:
                                            all_lang_option = page.locator(all_lang_selector).first
                                            if all_lang_option.is_visible(timeout=500):
                                                all_lang_option.click()
                                                print("  ✓ Changed language filter to 'All languages'")
                                                all_lang_clicked = True
                                                page.wait_for_timeout(1000)  # Wait for reviews to reload

                                                # Verify the change by checking if filter button now shows "All languages"
                                                try:
                                                    filter_text = filter_button.inner_text()
                                                    if "All" in filter_text or "all" in filter_text.lower():
                                                        print("  ✓ Confirmed: Language filter now shows all languages")
                                                    else:
                                                        print(f"  Filter text after change: {filter_text}")
                                                except Exception:
                                                    pass

                                                break
                                        except Exception:
                                            continue

                                    if not all_lang_clicked:
                                        print("  Could not find 'All languages' option")
                                        # Try to click away to close dropdown
                                        page.keyboard.press('Escape')

                                    language_filter_found = True
                                    break

                            except Exception:
                                continue

                        if not language_filter_found:
                            print("  No language filter found or already set to all languages")

                    except Exception as e:
                        print(f"  Error changing language filter: {e}")

                    # scroll to bottom to load all content
                    page.evaluate("""() => {
                        window.scrollTo(0, document.body.scrollHeight);
                    }""")

                    # Send ESC key after scrolling to close any popups that appear
                    send_escape_key(page, "after scrolling")

                    # Content loads after scroll

                    # Try to click "All reviews" button to load more content
                    try:
                        print("  Looking for 'All reviews' button...")
                        # Look for button containing "All reviews" text
                        all_reviews_button = page.locator('button:has-text("All reviews")')

                        if all_reviews_button.count() > 0:
                            print("  Clicking 'All reviews' button...")
                            all_reviews_button.first.click()

                            # Wait for network to be idle after clicking
                            try:
                                page.wait_for_load_state('networkidle', timeout=10000)
                            except Exception:
                                pass  # Continue even if networkidle times out

                            print("  Successfully clicked 'All reviews' button")

                            # Check for popups that might appear after loading reviews
                            close_promotional_popup(page) or aggressive_popup_check(page)

                        else:
                            print("  'All reviews' button not found")

                    except Exception as e:
                        print(f"  Error clicking 'All reviews' button: {e}")

                    # Click through all review pages to capture all reviews
                    try:
                        print("  Checking for review pagination...")
                        review_page_count = 1
                        max_review_pages = 20  # Safety limit to prevent infinite loops

                        while review_page_count < max_review_pages:
                            # Combined selector for Next page buttons
                            combined_next_selector = ', '.join([
                                'a[aria-label="Next page"]',
                                'a[data-smoke-attr="pagination-next-arrow"]',
                                'a[href*="Reviews-or"]:has(svg)',
                                '.IGLCo a[aria-label="Next page"]'
                            ])

                            try:
                                next_button = page.locator(combined_next_selector).first
                                if next_button.is_visible(timeout=800):
                                    # Check if button is not disabled
                                    is_disabled = next_button.evaluate(
                                        "(el) => el.classList.contains('disabled') || el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true'"
                                    )
                                    if not is_disabled:
                                        print(f"  Clicking to review page {review_page_count + 1}...")
                                        next_button.click()
                                        review_page_count += 1

                                        # Wait for new reviews to load
                                        try:
                                            page.wait_for_load_state('networkidle', timeout=4000)
                                        except Exception:
                                            page.wait_for_timeout(1000)  # Fallback wait

                                        # Send ESC key after pagination
                                        send_escape_key(page, f"after pagination to page {review_page_count}")

                                        # Check for popup after page load
                                        close_promotional_popup(page)
                                    else:
                                        print(f"  Next button is disabled. Total pages loaded: {review_page_count}")
                                        break
                                else:
                                    print(f"  No more review pages available. Total pages loaded: {review_page_count}")
                                    break
                            except Exception:
                                print(f"  No more review pages available. Total pages loaded: {review_page_count}")
                                break

                        if review_page_count >= max_review_pages:
                            print(f"  Reached maximum review page limit ({max_review_pages})")

                    except Exception as e:
                        print(f"  Error during review pagination: {e}")

                    # Try to click "See all photos" button to load photo gallery
                    try:
                        print("  Looking for 'See all photos' button...")

                        # Combined selector for efficiency
                        combined_photo_selector = ', '.join([
                            'button[data-automation="seeAllPhotosCountButton"]',
                            'button:has(svg):has-text("Photo")',
                            'button.rmyCe:has(svg)',
                            'button:has-text("See all photos")',
                            'button:has-text("View all photos")',
                            'button:has-text("Photos")'
                        ])

                        photo_button = page.locator(combined_photo_selector).first
                        if photo_button.is_visible(timeout=1000):
                            print("  Clicking 'See all photos' button...")
                            photo_button.scroll_into_view_if_needed()
                            photo_button.click()

                            # Wait for photo modal to load
                            try:
                                page.wait_for_load_state('networkidle', timeout=4000)
                            except Exception:
                                pass

                            # Check for popup that might appear over photo modal
                            if not close_promotional_popup(page):
                                aggressive_popup_check(page)

                            print("  Successfully clicked 'See all photos' button")

                            # Brief wait then close photo modal
                            page.wait_for_timeout(300)

                            # Combined selector for close buttons
                            combined_close_selector = ', '.join([
                                'button[aria-label="close"]',
                                'button[aria-label="Close"]',
                                'button.Vonfv[aria-label="close"]',
                                'button[type="button"][aria-label="close"]',
                                'button:has(svg):has-text("close")',
                                'button.BrOJk[aria-label="close"]'
                            ])

                            # Try to close modal
                            try:
                                close_button = page.locator(combined_close_selector).first
                                if close_button.is_visible(timeout=300):
                                    close_button.click(force=True)
                                    print("  ✓ Closed photo modal")
                                else:
                                    page.keyboard.press('Escape')
                                    print("  ✓ Closed photo modal with Escape key")
                                page.wait_for_timeout(300)
                            except Exception:
                                page.keyboard.press('Escape')  # Fallback

                            # Additional modal cleanup after photo interaction
                            close_all_modals(page)

                        else:
                            print("  'See all photos' button not found")

                    except Exception as e:
                        print(f"  Error clicking 'See all photos' button: {e}")

                    # Send ESC key before attempting features modal interaction
                    send_escape_key(page, "before features interaction")

                    # Try to click "See all features" link to load more content
                    modal_extracted_data = None  # Initialize modal data variable
                    try:
                        print("  Looking for 'See all features' link...")

                        # Multiple selectors for features links/buttons
                        feature_link_selectors = [
                            'a:has-text("See all features")',
                            'button:has-text("See all features")',
                            'a:has-text("Features")',
                            'button:has-text("Features")',
                            'a[href*="features"]',
                            'button[data-automation*="features"]',
                            'span:has-text("See all features")',
                            'div:has-text("See all features")'
                        ]

                        feature_link_found = False
                        for selector in feature_link_selectors:
                            try:
                                feature_link = page.locator(selector).first
                                if feature_link.is_visible(timeout=800):
                                    print(f"  Found features link with selector: {selector}")
                                    feature_link.scroll_into_view_if_needed()
                                    page.wait_for_timeout(200)
                                    feature_link.click()

                                    # Wait exactly 1 second for modal to fully load
                                    print("  Waiting 1 second for features modal to load...")
                                    page.wait_for_timeout(1000)

                                    # Extract data directly from the open modal
                                    print("  Extracting data from features modal...")
                                    try:
                                        modal_extracted_data = extract_modal_data(page)

                                        if modal_extracted_data:
                                            print(f"  ✓ Extracted modal data: {list(modal_extracted_data.keys())}")
                                            for key, value in modal_extracted_data.items():
                                                print(f"    - {key}: {value}")
                                        else:
                                            print("  ⚠ Modal extraction returned null/empty data")

                                    except Exception as e:
                                        print(f"  ❌ Exception during modal extraction: {e}")
                                        import traceback
                                        traceback.print_exc()

                                    print("  Successfully clicked 'See all features' link")
                                    feature_link_found = True
                                    break

                            except Exception:
                                continue

                        if not feature_link_found:
                            print("  'See all features' link not found")

                    except Exception as e:
                        print(f"  Error clicking 'See all features' link: {e}")
                        modal_extracted_data = None

                    # Close any features modal that might be open
                    try:
                        print("  Checking for open features modal to close...")

                        # Look for features modal/popup indicators
                        features_modal_selectors = [
                            'div[role="dialog"]:has-text("FEATURES")',
                            'div[role="dialog"]:has-text("Features")',
                            'div[class*="modal"]:has-text("FEATURES")',
                            'div[class*="popup"]:has-text("FEATURES")',
                            'div[aria-modal="true"]:has-text("FEATURES")'
                        ]

                        features_modal_found = False
                        for modal_selector in features_modal_selectors:
                            try:
                                modal = page.locator(modal_selector).first
                                if modal.is_visible(timeout=500):
                                    print("  Found open features modal, attempting to close...")

                                    # Try to find close button within the modal
                                    close_selectors = [
                                        'button[aria-label="Close"]',
                                        'button[aria-label="close"]',
                                        'button.BrOJk',
                                        'button:has(svg):has-text("×")',
                                        'button:has-text("×")',
                                        'button:has-text("✕")',
                                        '[role="button"][aria-label="Close"]'
                                    ]

                                    modal_closed = False
                                    for close_selector in close_selectors:
                                        try:
                                            close_btn = modal.locator(close_selector).first
                                            if close_btn.is_visible(timeout=300):
                                                close_btn.click()
                                                print("  ✓ Features modal closed")
                                                modal_closed = True
                                                page.wait_for_timeout(300)
                                                break
                                        except Exception:
                                            continue

                                    # If no close button worked, try Escape key
                                    if not modal_closed:
                                        page.keyboard.press('Escape')
                                        print("  ✓ Features modal closed with Escape key")
                                        page.wait_for_timeout(300)

                                    features_modal_found = True
                                    break

                            except Exception:
                                continue

                        if not features_modal_found:
                            print("  No open features modal detected")

                    except Exception as e:
                        print(f"  Error closing features modal: {e}")

                    # Final popup check before data extraction
                    print("  Final popup check before data extraction...")
                    close_promotional_popup(page) or aggressive_popup_check(page)

                    # Comprehensive modal cleanup to ensure everything is closed
                    close_all_modals(page)

                    # Final ESC key to ensure all modals are closed
                    send_escape_key(page, "final cleanup before data extraction")

                    # Final specific check for the persistent paetC popup
                    try:
                        print("  Final check for persistent paetC popup...")
                        paetc_popup = page.locator('div.paetC[role="dialog"]').first
                        if paetc_popup.is_visible(timeout=300):
                            print("  Found persistent paetC popup, forcing closure...")

                            # Try multiple close strategies for this specific popup
                            close_strategies = [
                                'div.JtGqK[data-automation="interstitialClose"] button.BrOJk',
                                'div[data-automation="interstitialClose"] button',
                                'button.BrOJk[aria-label="Close"]',
                                'button[aria-label="Close"]'
                            ]

                            popup_closed = False
                            for strategy in close_strategies:
                                try:
                                    close_btn = paetc_popup.locator(strategy).first
                                    if close_btn.is_visible(timeout=200):
                                        close_btn.click(force=True)
                                        print(f"  ✓ Persistent popup closed with: {strategy}")
                                        popup_closed = True
                                        page.wait_for_timeout(500)
                                        break
                                except Exception:
                                    continue

                            if not popup_closed:
                                # Force close with Escape multiple times
                                page.keyboard.press('Escape')
                                page.wait_for_timeout(200)
                                page.keyboard.press('Escape')
                                print("  ✓ Persistent popup dismissed with multiple Escape keys")
                                page.wait_for_timeout(500)
                        else:
                            print("  No persistent paetC popup found")

                    except Exception as e:
                        print(f"  Error in final paetC popup check: {e}")

                    # Extract restaurant website URL from the page
                    website_url = extract_restaurant_website(page)

                    # Extract phone number from the page
                    phone_number = extract_phone_number(page)

                    # Extract cuisines from the page (use modal data if available)
                    print(f"  Checking modal_extracted_data for cuisines: {modal_extracted_data}")
                    if modal_extracted_data and 'cuisines' in modal_extracted_data:
                        cuisines = modal_extracted_data['cuisines']
                        print(f"  ✓ Using modal cuisines: {cuisines}")
                    else:
                        print(f"  ⚠ No modal cuisines found, using page extraction. Modal data: {modal_extracted_data}")
                        cuisines = extract_cuisines(page)

                    # Extract meal types from the page (use modal data if available)
                    print(f"  Checking modal_extracted_data for meal_types: {bool(modal_extracted_data and 'meal_types' in modal_extracted_data)}")
                    if modal_extracted_data and 'meal_types' in modal_extracted_data:
                        meal_types = modal_extracted_data['meal_types']
                        print(f"  ✓ Using modal meal types: {meal_types}")
                    else:
                        print("  ⚠ No modal meal_types found, using page extraction")
                        meal_types = extract_meal_types(page)

                    # Extract features from the page (use modal data if available)
                    print(f"  Checking modal_extracted_data for features: {bool(modal_extracted_data and 'features' in modal_extracted_data)}")
                    if modal_extracted_data and 'features' in modal_extracted_data:
                        features = modal_extracted_data['features']
                        print(f"  ✓ Using modal features: {features}")
                    else:
                        print("  ⚠ No modal features found, using page extraction")
                        features = extract_features(page)

                    # Extract special diets from the page (use modal data if available)
                    print(f"  Checking modal_extracted_data for special_diets: {bool(modal_extracted_data and 'special_diets' in modal_extracted_data)}")
                    if modal_extracted_data and 'special_diets' in modal_extracted_data:
                        special_diets = modal_extracted_data['special_diets']
                        print(f"  ✓ Using modal special diets: {special_diets}")
                    else:
                        print("  ⚠ No modal special_diets found, using page extraction")
                        special_diets = extract_special_diets(page)

                    # Extract price from the page (use modal data if available)
                    price = None
                    if modal_extracted_data and 'price' in modal_extracted_data:
                        price = modal_extracted_data['price']
                        print(f"  ✓ Using modal price: {price}")

                    # Extract hours from the page
                    hours = extract_hours(page)

                    # Extract JSON-LD structured data from the restaurant page
                    jsonld_data = extract_restaurant_jsonld(page)

                    # Add website URL to JSON-LD data if found
                    if jsonld_data and website_url:
                        jsonld_data['website'] = website_url

                    # Add phone number to JSON-LD data if found
                    if jsonld_data and phone_number:
                        jsonld_data['phone'] = phone_number

                    # Add cuisines to JSON-LD data if found
                    if jsonld_data and cuisines:
                        jsonld_data['cuisines'] = cuisines

                    # Add meal types to JSON-LD data if found
                    if jsonld_data and meal_types:
                        jsonld_data['meal_types'] = meal_types

                    # Add features to JSON-LD data if found
                    if jsonld_data and features:
                        jsonld_data['features'] = features

                    # Add special diets to JSON-LD data if found
                    if jsonld_data and special_diets:
                        jsonld_data['special_diets'] = special_diets

                    # Add price to JSON-LD data if found
                    if jsonld_data and price:
                        jsonld_data['price'] = price

                    # Add hours to JSON-LD data if found
                    if jsonld_data and hours:
                        jsonld_data['hours'] = hours

                    # Determine scrape status
                    if jsonld_data and graphql_responses:
                        scrape_status = "success"
                    elif jsonld_data or graphql_responses:
                        scrape_status = "partial"
                    else:
                        scrape_status = "no_data_extracted"

                except Exception as e:
                    error_msg = f"Navigation/extraction error: {str(e)}"
                    print(f"  ERROR: {error_msg}")
                    errors.append(error_msg)
                    scrape_status = "failed"

    except Exception as e:
        error_msg = f"Browser initialization error: {str(e)}"
        print(f"  ERROR: {error_msg}")
        errors.append(error_msg)
        scrape_status = "browser_failed"

    # Store results in container
    result_container['graphql_responses'] = graphql_responses
    result_container['jsonld_data'] = jsonld_data
    result_container['scrape_status'] = scrape_status
    result_container['errors'] = errors


def scrape_restaurants(country="AT"):
    """Continuously scrape restaurants one at a time in an infinite loop.

    Args:
        country: Two-letter country code (e.g., "AT", "NL", "IT", "ES"). Defaults to "AT".
    """
    scraped_count = 0

    while True:
        # Get a single restaurant to scrape
        restaurants = get_restaurant_links(country=country)

        if not restaurants:
            print("\nNo restaurants available to scrape. Waiting 60 seconds before retry...")
            time.sleep(60)
            continue

        # Process the first (and should be only) restaurant
        restaurant = restaurants[0]
        scraped_count += 1

        print(f"\n{'='*70}")
        print(f"Restaurant #{scraped_count}: {restaurant['name']}")
        print(f"URL: {restaurant['tripadvisor_detail_page']}")
        print(f"{'='*70}\n")

        # Get country from nested city structure or use default
        city = restaurant.get('city', {})
        if city:
            country = city.get('country', {'name': 'Netherlands'})
        else:
            country = {'name': 'Netherlands'}
        print(f"Processing: {restaurant['name']}, {country['name'] if isinstance(country, dict) else country}")

        # Run browser scraping in a thread-safe manner
        result = run_browser_scraping_in_thread(restaurant)

        # Extract results from container
        graphql_responses = result.get('graphql_responses', [])
        jsonld_data = result.get('jsonld_data', None)
        scrape_status = result.get('scrape_status', 'failed')
        errors = result.get('errors', [])

        # Print summary of captured GraphQL data
        if graphql_responses:
            print("\n--- GraphQL Responses Summary ---")
            for idx, resp in enumerate(graphql_responses[:5], 1):
                print(f"Response #{idx}:")
                print(f"  URL: {resp['url'][:80]}...")
                if isinstance(resp['data'], dict):
                    if 'data' in resp['data']:
                        # Common GraphQL structure
                        ops = resp['data']['data'].keys() if resp['data']['data'] else []
                        print(f"  GraphQL operation keys: {list(ops) if ops else 'None'}")
                    else:
                        keys = list(resp['data'].keys())[:5]
                        print(f"  Response keys: {keys}...")
            if len(graphql_responses) > 5:
                print(f"... and {len(graphql_responses) - 5} more responses")

        # Extract website URL, phone, cuisines, meal types, features, special diets, and hours for top-level access
        website_url = None
        phone_number = None
        cuisines = None
        meal_types = None
        features = None
        special_diets = None
        hours = None
        if jsonld_data:
            if 'website' in jsonld_data:
                website_url = jsonld_data['website']
            if 'phone' in jsonld_data:
                phone_number = jsonld_data['phone']
            if 'cuisines' in jsonld_data:
                cuisines = jsonld_data['cuisines']
            if 'meal_types' in jsonld_data:
                meal_types = jsonld_data['meal_types']
            if 'features' in jsonld_data:
                features = jsonld_data['features']
            if 'special_diets' in jsonld_data:
                special_diets = jsonld_data['special_diets']
            if 'hours' in jsonld_data:
                hours = jsonld_data['hours']

        # Only save data if scraping was successful (complete data extracted)
        if scrape_status == "success":
            combined_data = {
                'restaurant_id': restaurant['id'],
                'restaurant_name': restaurant['name'],
                'tripadvisor_url': restaurant['tripadvisor_detail_page'],
                'website_url': website_url,
                'phone_number': phone_number,
                'cuisines': cuisines,
                'meal_types': meal_types,
                'features': features,
                'special_diets': special_diets,
                'hours': hours,
                'scrape_status': scrape_status,
                'errors': errors,
                'jsonld_data': jsonld_data,
                'graphql_responses': graphql_responses,
                'statistics': {
                    'total_graphql_responses': len(graphql_responses),
                    'has_jsonld_data': jsonld_data is not None,
                    'has_website_url': website_url is not None,
                    'has_phone_number': phone_number is not None,
                    'has_cuisines': cuisines is not None and len(cuisines) > 0,
                    'has_meal_types': meal_types is not None and len(meal_types) > 0,
                    'has_features': features is not None and len(features) > 0,
                    'has_special_diets': special_diets is not None and len(special_diets) > 0,
                    'has_hours': hours is not None and len(hours) > 0,
                    'error_count': len(errors)
                }
            }

            filename = f"scraped_data/full_restaurant_data_{restaurant['id']}.json"
            with open(filename, 'w') as f:
                json.dump(combined_data, f, indent=2)
            print(f"\n✓ Saved complete data to: {filename} (status: {scrape_status})")
        else:
            print(f"\n✗ Skipping JSON save for {restaurant['name']} - only partial/failed data extracted (status: {scrape_status})")
            combined_data = None  # Set to None when not saved

        # Also save just the JSON-LD data separately for easy access if available
        # if jsonld_data:
        #     jsonld_filename = f"scraped_data/restaurant_jsonld_{restaurant['id']}.json"
        #     with open(jsonld_filename, 'w') as f:
        #         json.dump(jsonld_data, f, indent=2)
        #     print(f"Saved JSON-LD data to: {jsonld_filename}")

        # Print final status
        print(f"Final status for {restaurant['name']}: {scrape_status}")
        if errors:
            print(f"Errors encountered: {len(errors)}")
            for error in errors:
                print(f"  - {error}")

        # Update the restaurant's last_scraped timestamp only if scrape was successful
        update_success = None  # API update is currently disabled
        if scrape_status == "success":
            update_success = update_restaurant_last_scraped(restaurant['id'], scrape_status)
            if not update_success:
                print(f"  WARNING: Failed to update last_scraped for restaurant {restaurant['id']}")
                errors.append("Failed to update last_scraped via API")
        else:
            print(f"  Skipping last_scraped update for restaurant {restaurant['id']} (status: {scrape_status})")
            update_success = None  # Indicates update was not attempted

        # Update the combined data to include API update status (only if data was saved)
        if combined_data:
            combined_data['api_update_success'] = update_success
            combined_data['timestamp'] = datetime.now().isoformat()

            # Re-save the file with updated information
            filename = f"scraped_data/full_restaurant_data_{restaurant['id']}.json"
            with open(filename, 'w') as f:
                json.dump(combined_data, f, indent=2)

        # Print completion message and wait before next restaurant
        print(f"\n{'='*70}")
        print(f"Completed restaurant #{scraped_count}: {restaurant['name']}")
        print(f"Status: {scrape_status}")
        print(f"Total scraped so far: {scraped_count}")
        print(f"{'='*70}")

        # Add a delay only if there was an error
        if scrape_status != "success":
            print("\nWaiting 10 seconds before fetching next restaurant (due to previous error)...")
            time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape TripAdvisor restaurant data")
    parser.add_argument(
        "--country",
        type=str,
        default="AT",
        help="Two-letter country code (e.g., AT, NL, IT, ES). Defaults to AT."
    )
    args = parser.parse_args()

    try:
        print(f"Starting infinite restaurant scraping loop for country: {args.country}")
        print("Press Ctrl+C to stop at any time\n")
        scrape_restaurants(country=args.country)
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user. Exiting gracefully...")
        print("All data has been saved to the scraped_data/ directory.")
    except Exception as e:
        print(f"\n\nUnexpected error occurred: {e}")
        print("All data has been saved to the scraped_data/ directory.")
