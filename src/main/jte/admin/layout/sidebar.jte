@import io.vertx.core.json.*

@param JsonArray collections

<!-- Sidebar -->
<div id="sidebar"
     class="w-64 tech-gradient shadow-2xl border-r border-blue-500/20 fixed lg:relative lg:translate-x-0 transform -translate-x-full transition-transform duration-300 ease-in-out z-30 h-full flex flex-col">
    <!-- Logo -->
    <div class="flex items-center justify-center h-16 border-b border-blue-500/30 flex-shrink-0">
        <a href="/" class="text-xl font-bold text-white tracking-wider">
<%--            <i class="fas fa-dragon mr-2"></i>KVectors Admin--%>
            KVectors 管理后台
        </a>
    </div>

    <!-- Navigation -->
    <nav class="flex-1 flex flex-col overflow-hidden">
        <div class="px-4 pt-4 mb-4 flex-shrink-0">
            <div class="flex items-center justify-between">
                <h3 class="text-xs font-semibold text-blue-300 uppercase tracking-wider">Vector Collections</h3>
                <a href="/collection/add"
                   class="text-blue-400 hover:text-blue-300 transition-colors duration-200 p-1 rounded hover:bg-blue-600/20">
                    <i class="fas fa-plus text-sm"></i>
                </a>
            </div>
        </div>

        <!-- Collections List - Scrollable -->
        <div class="flex-1 overflow-y-auto scrollbar-thin px-2 space-y-1">
            @for(int i = 0; i < collections.size(); ++i)
                <a href="/collection/detail/${collections.getString(i)}"
                   class="flex items-center px-4 py-3 text-sm font-medium text-slate-300 rounded-lg hover:bg-blue-600/20 hover:text-blue-300 transition-all duration-200 neon-border">
                    <i class="fas fa-bars text-cyan-400 mr-3"></i>
                    <span>${collections.getString(i)}</span>
                    <%--                <span class="ml-auto text-xs bg-cyan-500/20 text-cyan-300 px-2 py-1 rounded-full">125</span>--%>
                    <span class="ml-auto text-xs  text-cyan-300 px-2 py-1 rounded-full">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" class="w-6 h-6"><path
                                    d="M22 20V7L20 3H4L2 7.00353V20C2 20.5523 2.44772 21 3 21H21C21.5523 21 22 20.5523 22 20ZM4 9H20V19H4V9ZM5.236 5H18.764L19.764 7H4.237L5.236 5ZM15 11H9V13H15V11Z"></path></svg>
                    </span>
                </a>
            @endfor
        </div>
    </nav>
</div>

<!-- Overlay for mobile -->
<div id="overlay" class="fixed inset-0 bg-black bg-opacity-50 z-20 lg:hidden hidden"
     onclick="toggleSidebar()"></div>

