<script>
    function toggleSidebar() {
        const sidebar = document.getElementById('sidebar');
        const overlay = document.getElementById('overlay');

        sidebar.classList.toggle('-translate-x-full');
        overlay.classList.toggle('hidden');
    }

    function ok(message){
        Swal.fire({
          position: "top-end",
          icon: "success",
          title: message,
          showConfirmButton: false,
          timer: 1500
        });
    }

    function oops(message){
        Swal.fire({
          position: "top-end",
          icon: "error",
          title: message,
          showConfirmButton: false,
          timer: 1500
        });
    }
</script>