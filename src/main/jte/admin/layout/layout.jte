@import gg.jte.Content
@import io.vertx.core.json.*

@param String title
@param String subtitle
@param JsonObject json
@param Content content

<!DOCTYPE html>
<html lang="en">
@template.admin.layout.head(title=title)

<body class="bg-slate-900 text-white">
<div class="flex h-screen">
    @template.admin.layout.sidebar(collections = json.getJsonArray("collections"))

    <!-- Main Content -->
    <div class="flex-1 flex flex-col overflow-hidden lg:ml-0">
        <!-- Top Navigation -->
        <header class="tech-gradient shadow-lg border-b border-blue-500/30">
            <div class="flex items-center justify-between h-16 px-6">
                <div class="flex items-center">
                    <!-- Hamburger Menu (Mobile) -->
                    <button id="hamburger" class="lg:hidden text-blue-400 hover:text-blue-300 mr-4"
                            onclick="toggleSidebar()">
                        <i class="fas fa-bars text-xl"></i>
                    </button>
                    <h2 class="text-xl lg:text-2xl font-bold text-blue-300 tracking-wide">${subtitle}</h2>
                </div>
<%--                <div class="flex items-center space-x-4">--%>
<%--                    <div class="relative">--%>
<%--                        <button class="flex items-center text-sm rounded-full focus:outline-none focus:ring-2 focus:ring-blue-500 p-1">--%>
<%--                            <img class="h-8 w-8 rounded-full border-2 border-blue-400"--%>
<%--                                 src="https://afoo.me/icons/Griffin.png" alt="Profile">--%>
<%--                        </button>--%>
<%--                    </div>--%>
<%--                </div>--%>
            </div>
        </header>

        ${content}

        @template.admin.layout.copyright()
    </div>
</div>

@template.admin.layout.footerCommonScript()
@template.admin.layout.otherFooterScripts()
</body>
</html>
