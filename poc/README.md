# FileMonitor Experiment Report

This document presents a comparative study of four different implementations for file monitoring in Python. The implementations cover both native and third-party approaches and are evaluated based on key criteria such as **Accessibility**, **Reliability & Performance**, **Error Handling**, **Monitoring Capabilities**, and **Traceability/Rollback**.

## Table of Contents

- [Overview](#overview)
- [Implementation Options](#implementation-options)
  - [1. Standard Library Options (os & pathlib)](#1-standard-library-options-os--pathlib)
  - [2. PyFilesystem2 (fs)](#2-pyfilesystem2-fs)
  - [3. fsspec (Filesystem Spec)](#3-fsspec-filesystem-spec)
  - [4. Watchdog (Filesystem Monitoring)](#4-watchdog-filesystem-monitoring)
- [Comparison Summary](#comparison-summary)
- [Additional Considerations](#additional-considerations)
- [Conclusion and Recommendation](#conclusion-and-recommendation)

---

## Overview

Managing files on both local and network-attached storage (NAS) systems is a common requirement in many production environments. The chosen tool or implementation method should be able to:

- **Access** both local and remote (NAS) file systems efficiently.
- Provide **reliability** and fast performance when handling large directories.
- Offer clear **error handling** so that issues like unreachable file paths or permission errors can be managed gracefully.
- Support some form of **monitoring** (either via periodic polling or real-time notifications) to detect file changes.
- Enable **traceability and rollback** mechanisms so that file operations can be logged and potentially reversed if necessary.

This report compares four approaches that have been prototyped as part of our proof-of-concept (POC):

1. **Standard Library Options (os & pathlib)**
2. **PyFilesystem2 (fs)**
3. **fsspec (Filesystem Spec)**
4. **Watchdog (Filesystem Monitoring)**

---

## Implementation Options

### 1. Standard Library Options (os & pathlib)

**Description:**  
Utilizes Python’s built-in modules (`os`, `os.scandir`, `os.walk`, and `pathlib`) to list and monitor files on the filesystem. The NAS is assumed to be mounted on the Linux system as a local path.

**Pros:**
- **Built-In:** No external dependencies required.
- **Speed:** Very fast for recursive walks using `os.scandir`; ideal for large directories.
- **Simplicity:** Straightforward syntax and minimal overhead.

**Cons:**
- **Limited Connectivity:** NAS must be mounted; no native support for remote protocols.
- **Monitoring:** Does not support real-time notifications; requires polling.
- **Traceability:** No built-in logging or rollback—must be implemented manually.
- **Error Handling:** Relies on OS exceptions; additional coding is needed for retries or detailed logging.

---

### 2. PyFilesystem2 (fs)

**Description:**  
A third-party library that provides a unified, high-level API for working with various filesystem types (local, network, archives, etc.). It abstracts file paths into FS objects and supports connecting via protocols like FTP or SFTP (as well as using OS-mounted paths).

**Pros:**
- **Uniform API:** Code is backend-agnostic; works seamlessly for local or remote files.
- **Portability:** Abstracts differences in path separators, encoding, etc.
- **Enhanced Error Handling:** Offers a common exception hierarchy (e.g., `fs.errors.ResourceNotFound`).
- **Flexibility:** Supports multiple filesystem types and can be extended with wrappers (e.g., for logging).

**Cons:**
- **Additional Dependency:** Requires installation (`pip install fs`).
- **Overhead:** Slight performance overhead compared to native `os.scandir`.
- **Monitoring:** Does not provide real-time event monitoring; requires integration with other tools (e.g., Watchdog) for dynamic changes.
- **Rollback:** No built-in rollback; traceability must be implemented manually if file modifications occur.

---

### 3. fsspec (Filesystem Spec)

**Description:**  
Another third-party library aimed at providing a unified interface for file system operations, widely used in data engineering. It handles local, network, and even cloud-based storage uniformly and is optimized for block-wise I/O and caching.

**Pros:**
- **Unified Interface:** Works across many storage types (local, NAS, S3, etc.) using a single API.
- **Performance:** Optimized for large datasets with caching and efficient block-wise reads.
- **Transaction Support:** Offers atomic transaction features for write operations, which can help maintain data integrity.
- **Growing Ecosystem:** Widely used in distributed systems and data frameworks.

**Cons:**
- **Lower-Level Ergonomics:** Developers may need to be familiar with specific protocol details.
- **Error Handling:** Relies on backend libraries for errors; does not provide a unified error model like PyFilesystem.
- **Monitoring:** Does not provide native event notifications; requires polling or external monitoring.
- **Readability:** Documentation can be less user-friendly compared to PyFilesystem.

---

### 4. Watchdog (Filesystem Monitoring)

**Description:**  
A dedicated Python library for real-time monitoring of file system events. It uses OS-level notifications (such as inotify on Linux) to track changes like file creation, modification, and deletion.

**Pros:**
- **Real-Time Monitoring:** Provides immediate notifications of file system events.
- **Cross-Platform:** Supports multiple operating systems using their native mechanisms.
- **Traceability:** Excellent for audit trails; every event (creation, deletion, etc.) can be logged.
- **Low Overhead:** Efficient use of OS-level resources like inotify.

**Cons:**
- **Not a Listing Tool:** It does not list files on demand; it only monitors changes. An initial scan must be performed with another tool.
- **System Limits:** May require tuning (e.g., increasing inotify limits) when monitoring a large number of directories.
- **Standalone Limitations:** Does not provide built-in logging for file contents or performance metrics.
- **Dependency:** Adds an extra dependency (`pip install watchdog`).

---

## Comparison Summary

| Feature / Criterion                      | os & pathlib             | PyFilesystem2 (fs)          | fsspec                      | Watchdog                     |
|------------------------------------------|--------------------------|-----------------------------|-----------------------------|------------------------------|
| **Accessibility**                        | Built-in, no install     | Install via pip (`fs`)      | Install via pip (`fsspec`)   | Install via pip (`watchdog`) |
| **Performance & Reliability**            | Very fast with `os.scandir`; minimal overhead | Reliable for large trees; streaming directory walks | Optimized for large datasets; caching and block I/O | Real-time, efficient notifications via inotify |
| **Error Handling**                       | Relies on OS exceptions; manual handling required | Unified error hierarchy; clearer exception messages | Depends on backend libraries; less unified | Provides clear error notifications on setup issues |
| **Monitoring Capabilities**              | Polling only             | Polling only (requires external monitor for events) | Polling only; no built-in event notifications | True real-time event monitoring |
| **Traceability & Rollback**              | No built-in support; manual logging/rollback needed | Can be extended with wrappers; manual rollback required | No automatic logging; traceability via custom logging | Excellent for audit trails; rollback must be externally implemented |
| **Integration with Remote FS**           | NAS must be mounted locally | Supports remote protocols via FS URLs (FTP, SFTP, etc.) | Directly supports remote protocols (SMB, S3, etc.) | Works with mounted paths; relies on OS-level notifications |
| **Additional Overhead**                  | Minimal                  | Slight overhead due to abstraction layer | Slight overhead; lower-level integration required | Minimal (event-driven, but requires an observer thread) |

---

## Additional Considerations

- **Network Connectivity:**  
  For both PyFilesystem2 and fsspec, if the NAS is not pre-mounted, these tools can connect using protocols such as FTP, SFTP, or SMB. However, this adds complexity regarding authentication and network reliability.

- **Error & Exception Handling:**  
  While the standard library is straightforward, it lacks the unified error handling that PyFilesystem2 provides. fsspec’s error model depends on its backends, meaning you may need to catch a variety of exceptions.

- **Monitoring & Real-Time Needs:**  
  If real-time monitoring is critical, integrating Watchdog is essential. However, Watchdog only monitors changes and does not perform bulk listings; hence, a combination with another tool (e.g., os & pathlib or PyFilesystem2) is often required.

- **Traceability and Rollback:**  
  None of the solutions provide a one-click rollback. Traceability (logging file operations and changes) must be implemented at the application level, regardless of which file listing/monitoring tool is chosen.

- **Technical Debate:**  
  - **Performance vs. Abstraction:** Native os and pathlib methods are fastest due to their minimal overhead. In contrast, PyFilesystem2 and fsspec introduce abstraction layers that might slightly reduce raw performance but offer a more consistent and portable API across different storage types.
  - **Error Handling Uniformity:** PyFilesystem2 shines with its unified exception model, which can reduce the code complexity when handling errors. fsspec’s approach may require more granular exception management.
  - **Ecosystem and Future Integration:** fsspec is increasingly used in data engineering pipelines and might provide more out-of-the-box features (like transaction support for writes) if your application evolves to include more than file listing.

---

## Conclusion and Recommendation

Each solution offers distinct advantages and potential drawbacks:

- **Standard Library (os & pathlib):**  
  Best suited for environments where the NAS is pre-mounted, and the requirement is simple and fast file listing. It is lightweight and reliable but requires additional work for real-time monitoring and robust error handling.

- **PyFilesystem2:**  
  Provides a high-level, unified interface that is particularly useful if your application might expand to work with multiple storage backends (local, FTP, SFTP, etc.). The abstraction makes code maintenance easier at the cost of a slight performance hit and additional dependency.

- **fsspec:**  
  Ideal for data-intensive applications that already integrate with tools like Pandas or Dask. It offers excellent performance optimizations and supports a variety of storage protocols but requires careful handling of errors and lacks native monitoring features.

- **Watchdog:**  
  Essential if real-time monitoring is required. It works best when combined with one of the other methods to perform the initial file listing. Its strength is in providing immediate notifications and detailed audit trails of file system changes.

### Recommendation

For a comprehensive, production-ready FileMonitor solution:
- **If simplicity and speed are paramount**, and the NAS is already mounted, the **Standard Library (os & pathlib)** approach is a solid choice.
- **If flexibility and multi-backend support are required**, **PyFilesystem2** offers a robust, extensible API.
- **For data-centric applications** where large datasets and remote storage are involved, consider **fsspec**.
- **For dynamic, real-time monitoring needs**, integrate **Watchdog** with one of the above solutions to get immediate notifications of file changes.

This report should serve as a foundation for further POC experiments. Leadership can compare performance, error handling, and integration ease across these implementations to select the tool that best fits the operational requirements and long-term vision of the project.

---
