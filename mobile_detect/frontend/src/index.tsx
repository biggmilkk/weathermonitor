import React, { useEffect } from "react";
import { Streamlit, withStreamlitConnection } from "streamlit-component-lib";

function MobileDetect() {
  // Tell Streamlit to give us a tiny height (we render nothing)
  useEffect(() => {
    Streamlit.setFrameHeight(0);
  }, []);

  // Post width now + on resize/orientation changes
  useEffect(() => {
    const send = () => {
      const w = window.innerWidth || document.documentElement.clientWidth || 0;
      Streamlit.setComponentValue(w);
    };
    send(); // initial
    window.addEventListener("resize", send);
    window.addEventListener("orientationchange", send);
    return () => {
      window.removeEventListener("resize", send);
      window.removeEventListener("orientationchange", send);
    };
  }, []);

  return null; // no UI
}

export default withStreamlitConnection(MobileDetect);
