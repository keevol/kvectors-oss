@raw

<script>
    document.addEventListener("DOMContentLoaded", (event) => {
        // console.log("add other script logic here.");
    });

    htmx.on('htmx:sendError', function(evt) {
        alert("网络请求发送失败，请检查您的网络连接。");
    });
    htmx.on("htmx:responseError", function(evt) {
        const xhr = evt.detail.xhr;
        // alert(`请求失败，服务器返回状态码: ${xhr.status} - ${xhr.statusText}`);
        console.error("服务器响应错误:", evt.detail); // 在控制台输出详细信息以便调试
    })
    htmx.on('htmx:beforeSwap', function (evt) {
        if (evt.detail.xhr.status >= 400) {
            // 错误status也同样用htmx响应内容更新页面元素
            evt.detail.shouldSwap = true;
        }
    });

    htmx.on("goBackAfterSuccess", function(evt) {
        Swal.fire({
          title: "操作成功！",
          icon: "success"
        }).then(()=> {
            history.back();
        });
    })

    htmx.on("goHomepageAfterMessaging", function(evt) {
        Swal.fire({
          title: "操作成功！",
          icon: "success"
        }).then(()=> {
          location.href="/"
        });
    })

    htmx.on("done", function(evt){
        Swal.fire({
          title: decodeURIComponent(evt.detail.value),
          icon: "success"
        })
    })

    htmx.on("oops", function(evt){
        Swal.fire({
          title: decodeURIComponent(evt.detail.value),
          icon: "error"
        });
    })

</script>

@endraw